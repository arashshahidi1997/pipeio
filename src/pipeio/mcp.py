"""MCP tool functions for pipeio.

Called by projio's MCP server (``src/projio/mcp/pipeio.py``) to expose
pipeline management tools to AI agents.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


def _find_registry(root: Path) -> Path | None:
    """Locate the pipeline registry, checking .projio/pipeio/ first."""
    from pipeio.registry import find_registry
    return find_registry(root)


_NO_REGISTRY = {"error": "No pipeline registry found", "hint": "Run pipeio init"}


def _find_dot() -> str | None:
    """Find the graphviz ``dot`` binary.

    Checks PATH first, then known conda env locations.
    """
    import shutil
    import sys

    found = shutil.which("dot")
    if found:
        return found

    # Check alongside the running Python (same conda env)
    env_bin = Path(sys.executable).parent / "dot"
    if env_bin.exists():
        return str(env_bin)

    # Known conda env fallback paths
    for env in ("rag", "cogpy"):
        candidate = Path("/storage/share/python/environments/Anaconda3/envs") / env / "bin" / "dot"
        if candidate.exists():
            return str(candidate)

    return None


def _inject_dag_link_in_source(flow_dir: Path) -> None:
    """Inject ``![DAG](dag.svg)`` into the flow's source docs if not already present.

    Checks ``docs/index.md`` first, then ``docs/overview.md`` (legacy).
    The link uses a relative path that works both in the source location
    (where ``.build/dag.svg`` lives) and after ``docs_collect`` copies
    both files to ``docs/pipelines/<flow>/``.
    """
    docs_dir = flow_dir / "docs"
    if not docs_dir.is_dir():
        return

    for name in ("index.md", "overview.md"):
        candidate = docs_dir / name
        if not candidate.exists():
            continue
        text = candidate.read_text(encoding="utf-8")
        if "dag.svg" in text:
            return  # already has a DAG reference
        # Append DAG section
        text = text.rstrip("\n") + "\n\n## DAG\n\n![Rule DAG](dag.svg)\n"
        candidate.write_text(text, encoding="utf-8")
        return  # only inject into the first found


def _resolve_nb_path(flow_dir: Path, name: str) -> Path | None:
    """Resolve a notebook name to its .py path.

    Checks layouts in priority order:
    1. Workspace ``.src/`` layouts: ``notebooks/{workspace}/.src/{name}.py``
    2. Workspace direct layouts: ``notebooks/{workspace}/{name}.py`` (marimo)
    3. Flat ``.src/`` layout: ``notebooks/.src/{name}.py``
    4. Flat layout: ``notebooks/{name}.py``
    5. Subdirectory layout: ``notebooks/{name}/{name}.py`` (legacy)
    6. Fall back to ``notebook.yml`` entry matching
    """
    # Workspace .src/ layouts (explore and demo) — percent-format
    for workspace in ("explore", "demo"):
        ws_src = flow_dir / "notebooks" / workspace / ".src" / f"{name}.py"
        if ws_src.exists():
            return ws_src

    # Workspace direct layouts (explore and demo) — marimo lives here
    for workspace in ("explore", "demo"):
        ws_direct = flow_dir / "notebooks" / workspace / f"{name}.py"
        if ws_direct.exists():
            return ws_direct

    # Flat .src/ layout
    src = flow_dir / "notebooks" / ".src" / f"{name}.py"
    if src.exists():
        return src

    # Flat layout
    flat = flow_dir / "notebooks" / f"{name}.py"
    if flat.exists():
        return flat

    # Subdirectory layout (legacy)
    subdir = flow_dir / "notebooks" / name / f"{name}.py"
    if subdir.exists():
        return subdir

    # Fall back to notebook.yml entry matching
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            from pipeio.notebook.config import NotebookConfig
            cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for entry in cfg.entries:
                if Path(entry.path).stem == name:
                    candidate = flow_dir / entry.path
                    if candidate.exists():
                        return candidate
        except Exception:
            pass

    return None


def mcp_flow_list(root: Path, prefix: str | None = None) -> dict[str, Any]:
    """List flows, optionally filtered by pipe."""
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY
    registry = PipelineRegistry.from_yaml(registry_path)
    flows = registry.list_flows(prefix=prefix)
    return {"flows": [f.model_dump() for f in flows]}


def mcp_flow_status(root: Path, flow: str) -> dict[str, Any]:
    """Show status of a specific flow."""
    from pipeio.config import FlowConfig
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    result: dict[str, Any] = {
        "flow": entry.name,
        "flow": entry.name,
        "code_path": entry.code_path,
        "app_type": entry.app_type,
        "config_exists": False,
        "docs_exists": entry.doc_path is not None and Path(entry.doc_path).exists() if entry.doc_path else False,
    }

    if entry.config_path:
        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        result["config_exists"] = cfg_path.exists()

        if cfg_path.exists():
            try:
                cfg = FlowConfig.from_yaml(cfg_path)
                result["output_dir"] = cfg.output_dir
                result["registry_groups"] = cfg.groups()
            except Exception as exc:
                result["config_error"] = str(exc)

    # Count notebooks if notebook.yml exists
    if entry.config_path:
        nb_cfg_path = Path(entry.config_path).parent / "notebooks" / "notebook.yml"
        if not nb_cfg_path.is_absolute():
            nb_cfg_path = root / nb_cfg_path
        if nb_cfg_path.exists():
            try:
                from pipeio.notebook.config import NotebookConfig
                nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
                result["notebook_count"] = len(nb_cfg.entries)
            except Exception:
                pass

    # Detect contracts.py
    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    contracts_path = flow_dir / "contracts.py"
    if contracts_path.exists():
        result["has_contracts"] = True
        try:
            from pipeio.contracts import import_flow_module
            mod = import_flow_module(flow_dir, "contracts")
            if mod is not None:
                fns = [
                    fn for fn in ("validate_inputs", "validate_outputs")
                    if callable(getattr(mod, fn, None))
                ]
                result["contract_functions"] = fns
        except Exception:
            result["contract_functions"] = []
    else:
        result["has_contracts"] = False

    # Detect snakemake unit tests (.tests/)
    tests_dir = flow_dir / ".tests"
    if tests_dir.is_dir():
        unit_dir = tests_dir / "unit"
        rules_tested: list[str] = []
        if unit_dir.is_dir():
            rules_tested = sorted(
                d.name for d in unit_dir.iterdir() if d.is_dir()
            )
        result["unit_tests"] = {
            "exists": True,
            "rules_tested": rules_tested,
            "run_command": f"pytest {flow_dir / '.tests/'}",
        }
    else:
        result["unit_tests"] = {"exists": False}

    return result


def mcp_flow_deregister(
    root: Path,
    flow: str,
) -> dict[str, Any]:
    """Remove a flow from the pipeline registry.

    Only removes the registry entry — does NOT delete code, config, docs,
    or notebook files from the filesystem.

    Args:
        root: Project root.
        flow: Flow name.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        removed = registry.remove(flow)
    except KeyError as exc:
        return {"error": str(exc)}

    registry.to_yaml(registry_path)

    # Persist to ignore list so rescan doesn't re-register
    ignore_path = registry_path.parent / "registry_ignore.yml"
    ignored: list[str] = []
    if ignore_path.exists():
        raw = yaml.safe_load(ignore_path.read_text(encoding="utf-8")) or {}
        ignored = raw.get("ignore", [])
    flow_key = removed.name
    if flow_key not in ignored:
        ignored.append(flow_key)
        ignore_path.write_text(
            yaml.safe_dump({"ignore": ignored}, default_flow_style=False),
            encoding="utf-8",
        )

    return {
        "deregistered": True,
        "flow": removed.name,
        "flow": removed.name,
        "code_path": removed.code_path,
        "mods": list(removed.mods.keys()) if removed.mods else [],
        "note": "Registry entry removed. Added to registry_ignore.yml so rescan skips it.",
    }


def mcp_flow_fork(
    root: Path,
    flow: str,
    new_flow: str,
) -> dict[str, Any]:
    """Fork a flow: copy its code directory and register as a new flow.

    Creates a full copy of the flow's code (Snakefile, config, notebooks,
    scripts) under the new name.  The original flow is untouched.

    Args:
        root: Project root.
        flow: Source flow name.
        new_flow: Name for the forked flow.
    """
    import shutil

    from pipeio.registry import FlowEntry, PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)

    # Validate source exists
    try:
        source = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    # Check target doesn't conflict
    if new_flow in registry.flows:
        return {"error": f"Flow already exists: {new_flow}"}

    # Resolve source code directory
    src_dir = Path(source.code_path)
    if not src_dir.is_absolute():
        src_dir = root / src_dir

    if not src_dir.exists():
        return {"error": f"Source code directory not found: {src_dir}"}

    # Target is a sibling directory
    dst_dir = src_dir.parent / new_flow

    if dst_dir.exists():
        return {"error": f"Target directory already exists: {dst_dir}"}

    # Copy the code directory
    shutil.copytree(src_dir, dst_dir)

    # Build new registry entry with updated paths
    try:
        new_code_path = str(dst_dir.relative_to(root))
    except ValueError:
        new_code_path = str(dst_dir)

    new_config_path = None
    if source.config_path:
        new_config_path = source.config_path.replace(
            source.code_path, new_code_path
        )

    new_doc_path = None
    if source.doc_path:
        new_doc_path = source.doc_path.replace(flow, new_flow)

    new_entry = FlowEntry(
        name=new_flow,
        code_path=new_code_path,
        config_path=new_config_path,
        doc_path=new_doc_path,
        mods=source.mods.copy(),
        app_type=source.app_type,
    )

    registry.flows[target_key] = new_entry
    registry.to_yaml(registry_path)

    return {
        "forked": True,
        "source": f"{flow}",
        "target": target_key,
        "code_path": new_code_path,
        "mods": list(new_entry.mods.keys()) if new_entry.mods else [],
    }


def mcp_flow_new(
    root: Path,
    flow: str,
) -> dict[str, Any]:
    """Scaffold a new pipeline flow with standard directory structure.

    Creates the full flow scaffold under ``code/pipelines/<flow>/``:
    Snakefile, config.yml, publish.yml, Makefile, scripts/, rules/,
    docs/index.md, and notebook workspaces (explore + demo).

    Idempotent: only writes missing files, so it can augment an
    existing flow without overwriting.

    Args:
        root: Project root.
        flow: Flow name (must be a valid slug: lowercase, underscores).
    """
    from pipeio.registry import slug_ok

    if not slug_ok(flow):
        return {"error": f"Invalid flow name: {flow!r}. Use lowercase letters, digits, and underscores."}

    # Determine pipelines directory
    if (root / "code" / "pipelines").exists():
        pipelines_dir = root / "code" / "pipelines"
    elif (root / "pipelines").exists():
        pipelines_dir = root / "pipelines"
    else:
        pipelines_dir = root / "code" / "pipelines"

    flow_dir = pipelines_dir / flow
    is_new = not flow_dir.exists()

    if flow_dir.exists() and (flow_dir / "Snakefile").exists():
        is_new = False

    # Create directory structure (idempotent)
    flow_dir.mkdir(parents=True, exist_ok=True)
    created: list[str] = []

    for d in [
        "scripts",
        "rules",
        "docs",
        "notebooks/explore/.src",
        "notebooks/explore/.myst",
        "notebooks/demo/.src",
        "notebooks/demo/.myst",
    ]:
        path = flow_dir / d
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            created.append(d)

    # config.yml
    cfg_path = flow_dir / "config.yml"
    if not cfg_path.exists():
        cfg_path.write_text(
            f"# config for {flow}\n"
            f"input_dir: \"\"\n"
            f"output_dir: \"derivatives/{flow}\"\n"
            f"input_manifest: \"\"\n"
            f"output_manifest: \"derivatives/{flow}/manifest.yml\"\n"
            f"registry: {{}}\n",
            encoding="utf-8",
        )
        created.append("config.yml")

    # Snakefile
    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        snakefile.write_text(
            f"# Snakefile for {flow}\n"
            f"from pathlib import Path\n"
            f"\n"
            f"configfile: \"config.yml\"\n"
            f"\n"
            f"\n"
            f"rule all:\n"
            f"    input: []\n",
            encoding="utf-8",
        )
        created.append("Snakefile")

    # publish.yml
    pub_path = flow_dir / "publish.yml"
    if not pub_path.exists():
        pub_path.write_text(
            "dag: true\n"
            "report: false\n"
            "scripts: true\n",
            encoding="utf-8",
        )
        created.append("publish.yml")

    # Makefile
    makefile = flow_dir / "Makefile"
    if not makefile.exists():
        makefile.write_text(
            f"# Flow: {flow}\n"
            f"SHELL := /bin/bash\n"
            f"\n"
            f"# Project root (relative to this Makefile)\n"
            f"PROJECT_ROOT ?= $(shell git -C $(dir $(abspath $(lastword $(MAKEFILE_LIST)))) rev-parse --show-toplevel)\n"
            f"\n"
            f".PHONY: help run dry-run nb-status nb-sync nb-lab nb-publish\n"
            f"\n"
            f"help:\n"
            f"\t@echo \"Targets: run dry-run nb-status nb-sync nb-lab nb-publish\"\n"
            f"\n"
            f"run:\n"
            f"\tsnakemake --snakefile $(CURDIR)/Snakefile --directory $(PROJECT_ROOT) -j1\n"
            f"\n"
            f"dry-run:\n"
            f"\tsnakemake --snakefile $(CURDIR)/Snakefile --directory $(PROJECT_ROOT) -j1 -n\n"
            f"\n"
            f"nb-status:\n"
            f"\tpipeio nb status\n"
            f"\n"
            f"nb-sync:\n"
            f"\tpipeio nb sync --direction py2nb\n"
            f"\n"
            f"nb-lab:\n"
            f"\tpipeio nb lab\n"
            f"\n"
            f"nb-publish:\n"
            f"\tpipeio nb sync --direction py2nb --force\n"
            f"\tpipeio nb publish\n",
            encoding="utf-8",
        )
        created.append("Makefile")

    # notebook.yml + initial explore notebook
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if not nb_cfg_path.exists():
        nb_name = f"explore_{flow}"
        nb_path = flow_dir / "notebooks" / "explore" / ".src" / f"{nb_name}.py"
        nb_path.write_text(
            f"# ---\n"
            f"# jupyter:\n"
            f"#   jupytext:\n"
            f"#     text_representation:\n"
            f"#       format_name: percent\n"
            f"# ---\n"
            f"\n"
            f"# %% [markdown]\n"
            f"# # Explore {flow.replace('_', ' ').title()}\n"
            f"#\n"
            f"# Initial exploration notebook for {flow}.\n"
            f"\n"
            f"# %% [markdown]\n"
            f"# ## Setup\n"
            f"\n"
            f"# %%\n"
            f"from pathlib import Path\n"
            f"\n"
            f"# %% [markdown]\n"
            f"# ## Analysis\n"
            f"\n"
            f"# %%\n",
            encoding="utf-8",
        )

        nb_cfg = {
            "kernel": "",
            "publish": {"docs_dir": "", "prefix": "nb-"},
            "entries": [{
                "path": f"notebooks/explore/.src/{nb_name}.py",
                "kind": "explore",
                "description": f"Initial exploration for {flow}",
                "status": "draft",
                "pair_ipynb": True,
                "pair_myst": True,
            }],
        }
        nb_cfg_path.write_text(
            yaml.safe_dump(nb_cfg, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
        created.append("notebook.yml")
        created.append(f"notebooks/explore/.src/{nb_name}.py")

    # docs/index.md — flow overview (landing page)
    docs_index = flow_dir / "docs" / "index.md"
    if not docs_index.exists():
        # Also accept legacy overview.md
        legacy_overview = flow_dir / "docs" / "overview.md"
        if not legacy_overview.exists():
            # Read config for input/output paths if available
            _input_dir = ""
            _output_dir = f"derivatives/{flow}"
            _input_manifest = ""
            _output_manifest = f"derivatives/{flow}/manifest.yml"
            if cfg_path.exists():
                try:
                    _cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
                    _input_dir = _cfg.get("input_dir", _input_dir)
                    _output_dir = _cfg.get("output_dir", _output_dir)
                    _input_manifest = _cfg.get("input_manifest", _input_manifest)
                    _output_manifest = _cfg.get("output_manifest", _output_manifest)
                except Exception:
                    pass
            docs_index.write_text(
                f"# {flow} — Flow Overview\n"
                f"\n"
                f"## Purpose\n"
                f"\n"
                f"<!-- What does this flow produce? Why is it a single flow\n"
                f"     rather than split into multiple? What downstream flows\n"
                f"     consume its output? -->\n"
                f"\n"
                f"## Input\n"
                f"\n"
                f"- Input directory: `{_input_dir}`\n"
                f"- Input manifest: `{_input_manifest}`\n"
                f"\n"
                f"## Output\n"
                f"\n"
                f"- Output directory: `{_output_dir}`\n"
                f"- Output manifest: `{_output_manifest}`\n"
                f"\n"
                f"## Mod Chain\n"
                f"\n"
                f"<!-- Processing order with rationale. -->\n"
                f"\n"
                f"| Order | Mod | Purpose |\n"
                f"|-------|-----|---------|\n"
                f"| 1 | | |\n"
                f"\n"
                f"## Design Decisions\n"
                f"\n"
                f"<!-- Why this mod ordering? Why certain steps read from\n"
                f"     raw vs intermediate? -->\n"
                f"\n"
                f"## Known Gaps\n"
                f"\n"
                f"<!-- Flow-level issues, missing mods, planned additions.\n"
                f"     Remove entries as they are resolved. -->\n",
                encoding="utf-8",
            )
            created.append("docs/index.md")

    try:
        code_path = str(flow_dir.relative_to(root))
    except ValueError:
        code_path = str(flow_dir)

    return {
        "flow": flow,
        "code_path": code_path,
        "is_new": is_new,
        "created": created,
    }


def mcp_nb_status(
    root: Path,
    flow: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Show notebook sync and publication status.

    Args:
        root: Project root.
        flow: Filter to a specific flow (optional).
        name: Filter to a specific notebook name (optional).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    flow_statuses: list[dict[str, Any]] = []

    for entry in registry.list_flows():
        # Apply flow/name filters
        if flow and entry.name != flow:
            continue

        if not entry.config_path:
            continue
        flow_root = Path(entry.config_path).parent
        if not flow_root.is_absolute():
            flow_root = root / flow_root
        nb_cfg_path = flow_root / "notebooks" / "notebook.yml"
        if not nb_cfg_path.exists():
            continue

        try:
            from pipeio.notebook.config import NotebookConfig
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
        except Exception:
            continue

        notebooks: list[dict[str, Any]] = []
        for nb in nb_cfg.entries:
            nb_name = Path(nb.path).stem
            # Apply name filter
            if name and nb_name != name:
                continue

            nb_path = flow_root / nb.path if not Path(nb.path).is_absolute() else Path(nb.path)
            info: dict[str, Any] = {"name": nb_name}
            if nb.kind:
                info["kind"] = nb.kind
            if nb.description:
                info["description"] = nb.description
            info["status"] = nb.status
            kernel = nb_cfg.resolve_kernel(nb)
            if kernel:
                info["kernel"] = kernel

            # Check py file exists and get mtime
            py_path = nb_path if nb_path.suffix == ".py" else nb_path.with_suffix(".py")
            if py_path.exists():
                info["py_mtime"] = py_path.stat().st_mtime

            # Check ipynb exists and compare
            ipynb_path = nb_path.with_suffix(".ipynb")
            if ipynb_path.exists():
                info["ipynb_mtime"] = ipynb_path.stat().st_mtime
                if "py_mtime" in info:
                    info["synced"] = info["ipynb_mtime"] >= info["py_mtime"]
                    # Also detect reverse: ipynb newer means human edits
                    if info["ipynb_mtime"] > info.get("py_mtime", 0):
                        info["ipynb_has_newer_edits"] = True
                else:
                    info["synced"] = True
            else:
                info["synced"] = False

            notebooks.append(info)

        if notebooks:
            flow_statuses.append({
                "flow": entry.name,
                "notebooks": notebooks,
            })

    return {"flows": flow_statuses}


def mcp_nb_update(
    root: Path,
    flow: str,
    name: str,
    status: str | None = None,
    description: str | None = None,
    kind: str | None = None,
    mod: str | None = None,
    kernel: str | None = None,
) -> dict[str, Any]:
    """Update notebook metadata in notebook.yml.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook name (stem, without extension).
        status: New status (draft/active/stale/promoted/archived).
        description: New one-line description.
        kind: New kind (investigate/explore/demo/validate).
        mod: Associated mod name.
        kernel: Jupyter kernel name.
    """
    import yaml
    from pipeio.notebook.config import NotebookConfig
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_root = Path(entry.config_path).parent if entry.config_path else None
    if not flow_root:
        return {"error": f"No config_path for {flow}"}
    if not flow_root.is_absolute():
        flow_root = root / flow_root

    nb_cfg_path = flow_root / "notebooks" / "notebook.yml"
    if not nb_cfg_path.exists():
        return {"error": f"No notebook.yml found for {flow}"}

    try:
        nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
    except Exception as exc:
        return {"error": f"Failed to parse notebook.yml: {exc}"}

    # Find the entry by name
    target = None
    for nb in nb_cfg.entries:
        if Path(nb.path).stem == name:
            target = nb
            break

    if target is None:
        return {"error": f"Notebook {name!r} not found in notebook.yml"}

    # Update fields
    updated_fields: list[str] = []
    if status is not None:
        target.status = status
        updated_fields.append("status")
    if description is not None:
        target.description = description
        updated_fields.append("description")
    if kind is not None:
        target.kind = kind
        updated_fields.append("kind")
    if mod is not None:
        target.mod = mod
        updated_fields.append("mod")
    if kernel is not None:
        target.kernel = kernel
        updated_fields.append("kernel")

    if not updated_fields:
        return {"error": "No fields to update (pass status, description, kind, mod, or kernel)"}

    # Write back
    nb_cfg_path.write_text(
        yaml.dump(nb_cfg.model_dump(), sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )

    return {
        "notebook": name,
        "updated_fields": updated_fields,
        "entry": target.model_dump(),
    }


def mcp_nb_move(
    root: Path,
    flow_from: str,
    flow_to: str,
    name: str,
    kind: str = "",
) -> dict[str, Any]:
    """Move a notebook from one flow to another.

    Moves the ``.py`` source, paired ``.ipynb``, and ``.myst`` files.
    Updates ``notebook.yml`` in both source and target flows.

    Args:
        root: Project root.
        flow_from: Source flow name.
        flow_to: Destination flow name.
        name: Notebook basename (without extension).
        kind: Override workspace kind (investigate/explore/demo/validate).
              Defaults to the notebook's existing kind from notebook.yml.
    """
    import shutil

    import yaml
    from pipeio.notebook.config import NotebookConfig, NotebookEntry
    from pipeio.notebook.lifecycle import _nb_output_paths
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)

    # Resolve both flows
    try:
        entry_from = registry.get(flow_from)
    except (KeyError, ValueError) as exc:
        return {"error": f"Source flow: {exc}"}
    try:
        entry_to = registry.get(flow_to)
    except (KeyError, ValueError) as exc:
        return {"error": f"Target flow: {exc}"}

    from_dir = Path(entry_from.code_path)
    if not from_dir.is_absolute():
        from_dir = root / from_dir
    to_dir = Path(entry_to.code_path)
    if not to_dir.is_absolute():
        to_dir = root / to_dir

    # Find source notebook
    py_path = _resolve_nb_path(from_dir, name)
    if py_path is None:
        return {"error": f"Notebook {name!r} not found in flow {flow_from!r}"}

    # Load source notebook.yml and find the entry
    from_nb_cfg_path = from_dir / "notebooks" / "notebook.yml"
    if not from_nb_cfg_path.exists():
        return {"error": f"No notebook.yml in source flow {flow_from!r}"}

    from_nb_cfg = NotebookConfig.from_yaml(from_nb_cfg_path)
    source_entry = None
    source_idx = None
    for idx, nb in enumerate(from_nb_cfg.entries):
        if Path(nb.path).stem == name:
            source_entry = nb
            source_idx = idx
            break
    if source_entry is None:
        return {"error": f"Notebook {name!r} not in {flow_from} notebook.yml"}

    # Determine target workspace kind
    effective_kind = kind or source_entry.kind or "investigate"
    _EXPLORE_KINDS = {"investigate", "explore"}
    _DEMO_KINDS = {"demo", "validate"}
    if effective_kind in _EXPLORE_KINDS:
        workspace = "explore"
    elif effective_kind in _DEMO_KINDS:
        workspace = "demo"
    else:
        workspace = ""

    # Build target paths
    to_nb_dir = to_dir / "notebooks"
    if workspace:
        to_src_dir = to_nb_dir / workspace / ".src"
    else:
        to_src_dir = to_nb_dir / ".src"

    to_py_path = to_src_dir / f"{name}.py"
    if to_py_path.exists():
        return {"error": f"Notebook {name!r} already exists in flow {flow_to!r}"}

    # Compute all source file paths
    src_ipynb, src_myst = _nb_output_paths(py_path)

    to_src_dir.mkdir(parents=True, exist_ok=True)
    to_ipynb, to_myst = _nb_output_paths(to_py_path)

    # Move files
    moved_files: list[str] = []

    shutil.move(str(py_path), str(to_py_path))
    moved_files.append(str(to_py_path.relative_to(root)))

    if src_ipynb.exists():
        to_ipynb.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_ipynb), str(to_ipynb))
        moved_files.append(str(to_ipynb.relative_to(root)))

    if src_myst.exists():
        to_myst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_myst), str(to_myst))
        moved_files.append(str(to_myst.relative_to(root)))

    # Update source notebook.yml — remove entry
    from_nb_cfg.entries.pop(source_idx)
    from_nb_cfg_path.write_text(
        yaml.dump(from_nb_cfg.model_dump(), sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )

    # Update target notebook.yml — add entry with new path
    to_nb_cfg_path = to_nb_dir / "notebook.yml"
    if to_nb_cfg_path.exists():
        to_nb_cfg = NotebookConfig.from_yaml(to_nb_cfg_path)
    else:
        to_nb_dir.mkdir(parents=True, exist_ok=True)
        to_nb_cfg = NotebookConfig()

    new_entry = source_entry.model_copy()
    new_entry.path = str(to_py_path.relative_to(to_nb_dir))
    if kind:
        new_entry.kind = kind
    to_nb_cfg.entries.append(new_entry)
    to_nb_cfg_path.write_text(
        yaml.dump(to_nb_cfg.model_dump(), sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )

    return {
        "status": "ok",
        "name": name,
        "flow_from": entry_from.name,
        "flow_to": entry_to.name,
        "kind": effective_kind,
        "moved_files": moved_files,
    }


def mcp_mod_list(root: Path, flow: str | None = None) -> dict[str, Any]:
    """List mods for a specific flow."""
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    return {
        "flow": entry.name,
        "mods": {name: mod.model_dump() for name, mod in entry.mods.items()},
    }


def _resolve_mod_doc_path(
    root: Path, flow: str, mod: str,
) -> tuple[str | None, bool]:
    """Resolve documentation path for a mod.

    Checks these locations in order:
    1. ``docs/pipelines/{flow}/mods/{mod}/theory.md`` (faceted, preferred)
    2. ``docs/pipelines/{flow}/mods/{mod}.md`` (single-file)

    Returns (relative_path_or_None, exists_bool).
    """
    candidates = [
        root / "docs" / "pipelines" / flow / "mods" / mod / "theory.md",
        root / "docs" / "pipelines" / flow / "mods" / f"{mod}.md",
    ]
    for path in candidates:
        if path.exists():
            return str(path.relative_to(root)), True
    # Return the preferred convention path even if it doesn't exist yet
    return str(candidates[0].relative_to(root)), False


def mcp_mod_resolve(root: Path, modkeys: list[str]) -> dict[str, Any]:
    """Resolve modkeys into metadata.

    Modkey format: ``{flow}_mod-{mod}``
    """
    import re

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    pattern = re.compile(r"^(?:@)?(?P<flow>[^_]+)_mod-(?P<mod>.+)$")

    results: list[dict[str, Any]] = []
    for raw_key in modkeys:
        key = raw_key.strip()
        m = pattern.match(key)
        if not m:
            results.append({"input": raw_key, "error": f"Invalid modkey format: {key!r}"})
            continue

        flow = m.group("flow")
        mod = m.group("mod")
        try:
            entry = registry.get(flow)
        except (KeyError, ValueError) as exc:
            results.append({"input": raw_key, "error": str(exc)})
            continue

        mod_entry = entry.mods.get(mod)
        result: dict[str, Any] = {
            "input": raw_key,
            "modkey": f"{flow}_mod-{mod}",
            "flow": flow,
            "mod": mod,
            "found": mod_entry is not None,
        }
        if mod_entry:
            result["meta"] = mod_entry.model_dump()
            doc_path, doc_exists = _resolve_mod_doc_path(root, flow, mod)
            if mod_entry.doc_path and (root / mod_entry.doc_path).exists():
                result["doc_path"] = mod_entry.doc_path
                result["doc_exists"] = True
            else:
                result["doc_path"] = doc_path
                result["doc_exists"] = doc_exists
        else:
            doc_path, doc_exists = _resolve_mod_doc_path(root, flow, mod)
            result["doc_path"] = doc_path
            result["doc_exists"] = doc_exists
        results.append(result)

    return {"count": len(results), "results": results}


def mcp_mod_context(
    root: Path,
    flow: str | None = None,
    mod: str = "",
) -> dict[str, Any]:
    """Bundled read context for a single mod: rules, scripts, doc, config.

    Returns everything an agent needs to understand and work on a mod in one
    call.  Composes from existing internals — no new data model or cache.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        mod: Module name.
    """
    import re

    import yaml
    from pipeio.registry import PipelineRegistry

    if not mod:
        return {"error": "mod is required"}

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    mod_entry = entry.mods.get(mod)
    if mod_entry is None:
        return {"error": f"Mod {mod!r} not found in {entry.name}"}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # --- Rules: parse and filter to this mod ---
    rule_to_mod: dict[str, str] = {}
    for mname, me in entry.mods.items():
        for rname in me.rules:
            rule_to_mod[rname] = mname

    candidates: list[Path] = list(flow_dir.glob("*.smk"))
    snakefile = flow_dir / "Snakefile"
    if snakefile.exists():
        candidates.insert(0, snakefile)

    mod_rules: list[dict[str, Any]] = []
    for sf in candidates:
        try:
            text = sf.read_text(encoding="utf-8")
        except Exception:
            continue
        for rule_info in _parse_snakefile_rules(text):
            rname = rule_info["name"]
            rmatch = rule_to_mod.get(rname)
            if rmatch is None:
                m = _MOD_PREFIX_RE.match(rname)
                rmatch = m.group(1) if m else rname
            if rmatch == mod:
                rule_info["mod"] = mod
                rule_info["source_file"] = sf.name
                mod_rules.append(rule_info)

    # --- Scripts: read content for each unique script path ---
    scripts: dict[str, str] = {}
    for rule in mod_rules:
        script_path = rule.get("script")
        if script_path and script_path not in scripts:
            abs_script = flow_dir / script_path
            if abs_script.exists():
                try:
                    scripts[script_path] = abs_script.read_text(encoding="utf-8")
                except Exception:
                    scripts[script_path] = f"<read error>"

    # --- Doc: read mod documentation (faceted or legacy) ---
    doc_content: str | None = None
    doc_facets: dict[str, str] = {}
    doc_path_str, doc_exists = _resolve_mod_doc_path(root, entry.name, mod)

    # Check flow-local faceted docs: docs/{mod}/theory.md, spec.md, delta.md
    flow_local_doc_dir = flow_dir / "docs" / mod
    if flow_local_doc_dir.is_dir():
        for facet_name in ("theory", "spec", "delta"):
            facet_path = flow_local_doc_dir / f"{facet_name}.md"
            if facet_path.exists():
                try:
                    doc_facets[facet_name] = facet_path.read_text(encoding="utf-8")
                except Exception:
                    pass
        if doc_facets:
            doc_exists = True
            doc_path_str = str(flow_local_doc_dir.relative_to(root))
            # Primary doc content is theory
            doc_content = doc_facets.get("theory")

    # Fallback: registry doc_path
    if not doc_facets and mod_entry.doc_path:
        reg_doc = root / mod_entry.doc_path
        if reg_doc.is_dir():
            idx = reg_doc / "index.md"
            if idx.exists():
                doc_path_str = str(idx.relative_to(root))
                doc_exists = True
        elif reg_doc.is_file():
            doc_path_str = mod_entry.doc_path
            doc_exists = True

    if doc_content is None and doc_exists and doc_path_str:
        try:
            doc_content = (root / doc_path_str).read_text(encoding="utf-8")
        except Exception:
            pass

    # --- Config params: extract referenced sections ---
    config_params: dict[str, Any] = {}
    if entry.config_path:
        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        if cfg_path.exists():
            try:
                config = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
                # Collect top-level config keys referenced by mod's params
                for rule in mod_rules:
                    for _pname, pexpr in (rule.get("params") or {}).items():
                        # Extract config["section"] references
                        for key_match in re.finditer(r'config\["([^"]+)"\]', str(pexpr)):
                            section = key_match.group(1)
                            if section in config and section not in config_params:
                                config_params[section] = config[section]
            except Exception:
                pass

    # --- Bids signatures for mod's output groups ---
    bids_signatures: dict[str, dict[str, str]] = {}
    if entry.config_path:
        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        if cfg_path.exists():
            try:
                config = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
                pybids_inputs = config.get("pybids_inputs") or {}
                registry_data = config.get("registry") or {}
                # Only include groups whose base_input rules overlap with this mod's rules
                mod_rule_names = {r["name"] for r in mod_rules}
                for group_name, group in registry_data.items():
                    if not isinstance(group, dict):
                        continue
                    members = group.get("members") or {}
                    group_sigs: dict[str, str] = {}
                    for member_name, member in members.items():
                        if isinstance(member, dict):
                            group_sigs[member_name] = _render_bids_signature(
                                group, pybids_inputs, member
                            )
                    if group_sigs:
                        bids_signatures[group_name] = group_sigs
            except Exception:
                pass

    return {
        "flow": entry.name,
        "flow": entry.name,
        "mod": mod,
        "mod_meta": mod_entry.model_dump(),
        "rules": mod_rules,
        "scripts": scripts,
        "doc_path": doc_path_str,
        "doc": doc_content,
        "doc_facets": doc_facets or None,
        "config_params": config_params,
        "bids_signatures": bids_signatures,
    }


def mcp_registry_scan(root: Path) -> dict[str, Any]:
    """Scan the filesystem for pipelines and rebuild the registry."""
    from pipeio.registry import PipelineRegistry

    # Determine pipelines directory (same logic as CLI)
    if (root / "code" / "pipelines").exists():
        pipelines_dir = root / "code" / "pipelines"
    elif (root / "pipelines").exists():
        pipelines_dir = root / "pipelines"
    else:
        return {"error": "No pipelines directory found (checked code/pipelines/ and pipelines/)"}

    docs_dir = None
    if (root / "docs" / "explanation" / "pipelines").exists():
        docs_dir = root / "docs" / "explanation" / "pipelines"

    # Load ignore list from registry_ignore.yml
    ignore: set[str] = set()
    for cfg_dir in (root / ".projio" / "pipeio", root / ".pipeio"):
        ignore_path = cfg_dir / "registry_ignore.yml"
        if ignore_path.exists():
            raw = yaml.safe_load(ignore_path.read_text(encoding="utf-8")) or {}
            ignore = set(raw.get("ignore", []))
            break

    registry = PipelineRegistry.scan(pipelines_dir, docs_dir=docs_dir, ignore=ignore or None)

    # Write to registry file
    for candidate in (
        root / ".projio" / "pipeio" / "registry.yml",
        root / ".pipeio" / "registry.yml",
    ):
        if candidate.parent.exists():
            registry.to_yaml(candidate)
            break
    else:
        # Fallback: create .pipeio/ if neither exists
        out_dir = root / ".pipeio"
        out_dir.mkdir(exist_ok=True)
        registry.to_yaml(out_dir / "registry.yml")

    flows = registry.list_flows()
    total_mods = sum(len(f.mods) for f in flows)
    return {
        "scanned": str(pipelines_dir),
        "pipes": len(registry.list_flows()),
        "flows": len(flows),
        "mods": total_mods,
        "flow_details": [
            {
                "flow": f.name,
                "flow": f.name,
                "app_type": f.app_type,
                "has_config": f.config_path is not None,
                "has_docs": f.doc_path is not None,
                "mod_count": len(f.mods),
            }
            for f in flows
        ],
    }


def mcp_modkey_bib(
    root: Path,
    output_path: str | None = None,
    project_name: str | None = None,
) -> dict[str, Any]:
    """Generate a BibTeX file with ``@misc`` entries for all registered mods.

    Each mod gets a citekey of the form ``pipe-X_flow-Y_mod-Z`` so manuscripts
    can reference pipeline components as ``[@pipe-X_flow-Y_mod-Z]``.

    Parameters
    ----------
    root : Path
        Project root.
    output_path : str, optional
        Where to write the .bib file, relative to *root*.
        Default: ``.projio/pipeio/modkey.bib``.
    project_name : str, optional
        Author/project name for the bib entries.  Defaults to the project
        directory name.
    """
    from datetime import datetime, timezone

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    flows = registry.list_flows()
    if not flows:
        return {"error": "Registry contains no flows"}

    author = project_name or root.name
    year = datetime.now(tz=timezone.utc).strftime("%Y")
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    entries: list[str] = []
    count = 0
    for flow_entry in flows:
        flow = flow_entry.name
        for mod_name, mod_entry in sorted(flow_entry.mods.items()):
            modkey = f"{flow}_mod-{mod_name}"
            doc_path, _ = _resolve_mod_doc_path(root, flow, mod_name)
            rules_str = ", ".join(mod_entry.rules) if mod_entry.rules else ""
            entry = (
                f"@misc{{{modkey},\n"
                f"  title     = {{{author} mod: flow={flow} mod={mod_name}}},\n"
                f"  author    = {{{author}}},\n"
                f"  year      = {{{year}}},\n"
                f"  note      = {{doc_path={doc_path}; rules={rules_str}}},\n"
                f"}}"
            )
            entries.append(entry)
            count += 1

    header = (
        f"% modkey.bib — auto-generated by pipeio ({timestamp})\n"
        f"% {count} mod entries from {len(flows)} flows\n"
        f"% Output: .projio/pipeio/modkey.bib\n"
        f"% Re-generate with: pipeio_modkey_bib()\n\n"
    )

    bib_content = header + "\n\n".join(entries) + "\n"

    # Write the file
    rel_path = output_path or ".projio/pipeio/modkey.bib"
    out = root / rel_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(bib_content, encoding="utf-8")

    return {
        "path": rel_path,
        "entries": count,
        "flows": len(flows),
        "modkeys": [
            f"{f.name}_mod-{m}"
            for f in flows
            for m in sorted(f.mods)
        ],
    }


def mcp_docs_collect(root: Path) -> dict[str, Any]:
    """Collect flow-local docs and notebook outputs into docs/pipelines/ (build artifact, gitignored)."""
    from pipeio.docs import docs_collect

    try:
        collected = docs_collect(root)
    except ImportError as exc:
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": str(exc)}

    return {
        "collected": len(collected),
        "files": collected,
    }


def mcp_docs_nav(root: Path, *, write: bool = True) -> dict[str, Any]:
    """Generate MkDocs nav for docs/pipelines/ and write monorepo sub-mkdocs.yml.

    When ``write=True`` (default), writes ``docs/pipelines/mkdocs.yml`` for the
    mkdocs-monorepo-plugin.  The root ``mkdocs.yml`` includes it via::

        - Pipelines: '!include ./docs/pipelines/mkdocs.yml'

    This is set up automatically by ``projio sync``.
    """
    from pipeio.docs import docs_nav

    fragment = docs_nav(root, write=write)
    sub_mkdocs = root / "docs" / "pipelines" / "mkdocs.yml"
    return {
        "nav_fragment": fragment,
        "sub_mkdocs": str(sub_mkdocs.relative_to(root)) if sub_mkdocs.exists() else None,
        "written": write and sub_mkdocs.exists(),
    }


def mcp_contracts_validate(
    root: Path,
    *,
    run: bool = False,
    run_kwargs: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validate I/O contracts for all flows in the registry.

    With *run=False* (default): checks config structure and discovers
    ``contracts.py`` modules.

    With *run=True*: also executes discovered contract functions.
    *run_kwargs* maps function names to keyword arguments (paths).
    """
    from pipeio.contracts import validate_flow_contracts

    results = validate_flow_contracts(root, run=run, run_kwargs=run_kwargs)
    if not results:
        return _NO_REGISTRY

    flow_results = []
    for fv in results:
        entry: dict[str, Any] = {
            "flow": fv.flow_id,
            "valid": fv.ok,
            "passed": fv.passed,
            "warnings": fv.warnings,
            "errors": fv.errors,
            "has_contracts": fv.has_contracts,
            "contract_functions": fv.contract_functions,
        }
        if fv.contract_results:
            entry["contract_results"] = fv.contract_results
        flow_results.append(entry)

    all_valid = all(fv.ok for fv in results)
    return {
        "valid": all_valid,
        "flows": flow_results,
    }


def _nb_template(
    *,
    name: str,
    flow: str,
    kind: str,
    description: str,
    config_path: str = "",
    groups: list[str] | None = None,
    output_dir: str = "",
    compute_lib: str = "",
) -> list[str]:
    """Generate kind-aware notebook template lines.

    Returns a list of strings (lines) for a percent-format .py notebook.
    Template varies by kind:
    - investigate/explore: config loading, registry groups, data iteration, analysis
    - demo/validate: load final outputs, visualization, summary
    """
    L: list[str] = []

    # --- Header ---
    L.extend([
        "# ---",
        "# jupyter:",
        "#   jupytext:",
        "#     text_representation:",
        "#       format_name: percent",
        "# ---",
        "",
        "# %% [markdown]",
        f"# # {name.replace('_', ' ').title()}",
        "#",
        f"# {description}",
        "",
    ])

    # --- Setup cell ---
    L.extend([
        "# %% [markdown]",
        "# ## Setup",
        "",
        "# %%",
        "from pathlib import Path",
        "",
        "import yaml",
        "",
    ])

    if compute_lib:
        L.append(f"import {compute_lib}")
        L.append("")

    # --- Config loading ---
    if config_path:
        L.extend([
            f'config_path = Path("{config_path}")',
            "with open(config_path) as f:",
            "    config = yaml.safe_load(f)",
            "",
            f'output_dir = Path(config.get("output_dir", ""))',
            "",
        ])
    elif output_dir:
        L.append(f'output_dir = Path("{output_dir}")')
        L.append("")

    if groups:
        L.append(f"# Available registry groups: {', '.join(groups)}")
        L.append("")

    # --- Kind-specific sections ---
    if kind in ("investigate", "explore"):
        # Exploration: iterate subjects, load data, analyze
        L.extend([
            "# %% [markdown]",
            f"# ## Data Loading",
            "#",
            f"# Load pipeline outputs for exploration.",
            "",
            "# %%",
            "# Example: iterate over subjects",
            "# subjects = sorted(output_dir.glob('sub-*'))",
            "# for sub_dir in subjects:",
            "#     print(sub_dir.name)",
            "",
            "# %% [markdown]",
            "# ## Analysis",
            "",
            "# %%",
            "",
            "# %% [markdown]",
            "# ## Findings",
            "#",
            "# Summarize results here. These feed into theory.md.",
            "",
            "# %%",
            "",
        ])
    elif kind in ("demo", "validate"):
        # Demo: load final outputs, visualize, summarize
        L.extend([
            "# %% [markdown]",
            f"# ## Load Outputs",
            "#",
            f"# Load final pipeline outputs for demonstration.",
            "",
            "# %%",
        ])
        if groups:
            L.append(f"# Registry groups available: {', '.join(groups)}")
            L.append("# Load from output_dir / group / subject / ...")
        L.extend([
            "",
            "# %% [markdown]",
            "# ## Visualization",
            "",
            "# %%",
            "",
            "# %% [markdown]",
            "# ## Summary",
            "#",
            "# Key results demonstrated above.",
            "",
            "# %%",
            "",
        ])
    else:
        # Generic fallback
        L.extend([
            "# %% [markdown]",
            "# ## Analysis",
            "",
            "# %%",
            "",
        ])

    return L


def mcp_nb_create(
    root: Path,
    flow: str,
    name: str,
    kind: str = "investigate",
    description: str = "",
    format: str = "",
) -> dict[str, Any]:
    """Scaffold a new notebook for a flow.

    Creates a ``.py`` notebook (percent-format or marimo) with bootstrap
    cells and registers it in ``notebook.yml``.

    Percent-format notebooks are placed in ``.src/`` (agent territory).
    Marimo notebooks are placed directly in the workspace dir (the ``.py``
    IS the human interface).

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook name (e.g. ``investigate_noise``).
        kind: Prefix convention (investigate, explore, demo, interactive).
        description: One-line purpose, injected as header comment.
        format: Notebook format (``"percent"``, ``"marimo"``, or ``""``
            for auto-select based on kind — ``interactive`` defaults
            to marimo, others to percent).
    """
    from pipeio.config import FlowConfig
    from pipeio.notebook.config import NotebookConfig, NotebookEntry
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    # Resolve flow directory
    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    nb_dir = flow_dir / "notebooks"

    # Resolve format: interactive defaults to marimo, others to percent
    effective_format = format
    if not effective_format:
        effective_format = "marimo" if kind == "interactive" else "percent"

    # Route to workspace directory based on kind
    _EXPLORE_KINDS = {"investigate", "explore", "interactive"}
    _DEMO_KINDS = {"demo", "validate"}
    if kind in _EXPLORE_KINDS:
        workspace = "explore"
    elif kind in _DEMO_KINDS:
        workspace = "demo"
    else:
        workspace = ""

    # Marimo files live in workspace dir directly (the .py IS the human interface)
    # Percent-format files live in .src/ (hidden, .ipynb is the human interface)
    if effective_format == "marimo":
        if workspace:
            target_dir = nb_dir / workspace
        else:
            target_dir = nb_dir
    else:
        if workspace:
            target_dir = nb_dir / workspace / ".src"
        else:
            target_dir = nb_dir / ".src"
    target_dir.mkdir(parents=True, exist_ok=True)

    nb_path = target_dir / f"{name}.py"
    if nb_path.exists():
        return {"error": f"Notebook already exists: {nb_path.relative_to(root)}"}

    # Load flow config for registry groups and output dir
    groups: list[str] = []
    output_dir = ""
    config_path_str = entry.config_path
    if config_path_str:
        cfg_path = Path(config_path_str)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        if cfg_path.exists():
            try:
                cfg = FlowConfig.from_yaml(cfg_path)
                groups = cfg.groups()
                output_dir = cfg.output_dir
            except Exception:
                pass

    # Discover project compute library via codio (if available)
    compute_lib: str = ""
    try:
        from codio import load_config as codio_load_config  # type: ignore[import]
        from codio import Registry as CodioRegistry  # type: ignore[import]
        codio_cfg = codio_load_config(root)
        codio_reg = CodioRegistry.load(codio_cfg)
        internals = [lib for lib in codio_reg.list() if lib.kind == "internal"]
        if internals:
            compute_lib = internals[0].runtime_import or internals[0].name
    except Exception:
        pass

    # Compute relative config path from notebook dir
    rel_cfg_str = ""
    if config_path_str:
        try:
            abs_cfg = (root / config_path_str).resolve()
            rel_cfg_str = str(abs_cfg.relative_to(target_dir.resolve()))
        except ValueError:
            rel_cfg_str = config_path_str

    # Generate template via backend
    from pipeio.notebook.backend import get_backend
    backend = get_backend(effective_format)
    desc_text = description or f"{kind.title()} notebook for {flow}"
    content = backend.template(
        name=name,
        flow=flow,
        kind=kind,
        description=desc_text,
        config_path=rel_cfg_str,
        groups=groups,
        output_dir=output_dir,
        compute_lib=compute_lib,
    )

    nb_path.write_text(content, encoding="utf-8")

    # Register in notebook.yml
    nb_cfg_path = nb_dir / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
        except Exception:
            nb_cfg = NotebookConfig()
    else:
        nb_cfg = NotebookConfig()

    # Marimo: workspace dir directly. Percent: .src/ subdir.
    if effective_format == "marimo":
        if workspace:
            rel_nb = f"notebooks/{workspace}/{name}.py"
        else:
            rel_nb = f"notebooks/{name}.py"
    else:
        if workspace:
            rel_nb = f"notebooks/{workspace}/.src/{name}.py"
        else:
            rel_nb = f"notebooks/.src/{name}.py"

    existing_paths = {e.path for e in nb_cfg.entries}
    if rel_nb not in existing_paths:
        new_entry_kwargs: dict[str, Any] = {
            "path": rel_nb,
            "kind": kind,
            "description": description,
            "status": "active",
        }
        if effective_format == "marimo":
            new_entry_kwargs["format"] = "marimo"
        else:
            new_entry_kwargs.update(pair_ipynb=True, pair_myst=True, publish_myst=True)
        nb_cfg.entries.append(NotebookEntry(**new_entry_kwargs))
        import yaml
        nb_cfg_path.write_text(
            yaml.dump(nb_cfg.model_dump(), sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )

    try:
        result_path = str(nb_path.relative_to(root))
    except ValueError:
        result_path = str(nb_path)

    return {
        "created": result_path,
        "flow": flow,
        "flow": flow,
        "name": name,
        "kind": kind,
        "registry_groups": groups,
        "notebook_yml_updated": rel_nb not in existing_paths,
    }


def mcp_nb_sync(
    root: Path,
    flow: str,
    name: str,
    formats: list[str] | None = None,
    python_bin: str | None = None,
    direction: str = "py2nb",
    force: bool = False,
) -> dict[str, Any]:
    """Sync a specific notebook (jupytext pair + convert).

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
        formats: Which formats to produce (default: ['ipynb', 'myst']).
            Only used for py2nb direction.
        python_bin: Python binary where jupytext is installed (optional).
        direction: 'py2nb' (default) regenerates .ipynb/.md from .py.
            'nb2py' updates .py from the paired .ipynb (for human edits).
        force: If False, skip files already up-to-date (mtime check).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)

    # For nb2py direction, the .ipynb must exist (not the .py)
    if direction == "py2nb" and py_path is None:
        return {"error": f"Notebook not found: {name}"}
    if direction == "nb2py":
        # For nb2py, the .py may not exist yet — resolve from ipynb
        if py_path is None:
            # Try subdirectory layout for the ipynb
            for candidate in (
                flow_dir / "notebooks" / f"{name}.py",
                flow_dir / "notebooks" / name / f"{name}.py",
            ):
                if candidate.with_suffix(".ipynb").exists():
                    py_path = candidate
                    break
        if py_path is None:
            return {"error": f"Paired notebook not found: {name}.ipynb"}
        # Use layout-aware path computation: for workspace .src/ layouts,
        # the .ipynb lives in the parent dir (notebooks/demo/foo.ipynb),
        # not alongside the .py (notebooks/demo/.src/foo.ipynb).
        try:
            from pipeio.notebook.lifecycle import _nb_output_paths
            ipynb_path, _ = _nb_output_paths(py_path)
        except ImportError:
            ipynb_path = py_path.with_suffix(".ipynb")
        if not ipynb_path.exists():
            return {"error": f"Paired notebook not found: {ipynb_path.name}"}

    try:
        from pipeio.notebook.lifecycle import nb_sync_one
    except ImportError as exc:
        return {"error": str(exc)}

    # Resolve kernel from notebook.yml (entry-level > flow-level)
    kernel = ""
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            from pipeio.notebook.config import NotebookConfig
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for nb in nb_cfg.entries:
                if Path(nb.path).stem == name:
                    kernel = nb_cfg.resolve_kernel(nb)
                    break
        except Exception:
            pass

    result = nb_sync_one(
        py_path,
        direction=direction,
        formats=formats,
        force=force,
        kernel=kernel,
        python_bin=python_bin,
    )

    # Relativize paths for cleaner output
    def _rel(p: str) -> str:
        try:
            return str(Path(p).relative_to(root))
        except ValueError:
            return p

    if "source" in result:
        result["source"] = _rel(result["source"])
    for key in ("generated", "updated"):
        if key in result:
            result[key] = [_rel(p) for p in result[key]]

    return result


def mcp_nb_sync_flow(
    root: Path,
    flow: str,
    direction: str = "py2nb",
    force: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Batch-sync all notebooks in a flow.

    Args:
        root: Project root.
        flow: Flow name.
        direction: 'py2nb' or 'nb2py'.
        force: If True, sync even if up-to-date.
        python_bin: Python binary where jupytext is installed.
    """
    from pipeio.notebook.config import NotebookConfig
    from pipeio.notebook.lifecycle import nb_sync_one
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if not nb_cfg_path.exists():
        return {"error": f"No notebook.yml found for {flow}"}

    nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
    results: list[dict[str, Any]] = []

    for nb in nb_cfg.entries:
        name = Path(nb.path).stem
        py_path = _resolve_nb_path(flow_dir, name)
        if py_path is None and direction == "py2nb":
            results.append({"name": name, "skipped": True, "reason": "py not found"})
            continue
        if py_path is None:
            py_path = flow_dir / nb.path  # best guess for nb2py

        kernel = nb_cfg.resolve_kernel(nb)
        result = nb_sync_one(
            py_path, direction=direction, force=force,
            kernel=kernel, python_bin=python_bin,
        )
        result["name"] = name
        results.append(result)

    synced = [r for r in results if r.get("synced")]
    skipped = [r for r in results if r.get("skipped")]

    return {
        "flow": flow,
        "flow": flow,
        "direction": direction,
        "total": len(results),
        "synced": len(synced),
        "skipped": len(skipped),
        "results": results,
    }


def mcp_nb_diff(
    root: Path,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Show sync state between .py and paired .ipynb for a notebook.

    Returns which file is newer, whether they're in sync, and the
    recommended sync direction. Useful before deciding whether to
    sync and in which direction.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        # For diff, try the expected paths anyway (may both be missing)
        py_path = flow_dir / "notebooks" / name / f"{name}.py"
        if not py_path.parent.exists():
            py_path = flow_dir / "notebooks" / f"{name}.py"

    from pipeio.notebook.lifecycle import nb_diff

    result = nb_diff(py_path)

    # Relativize paths
    def _rel(p: str) -> str:
        try:
            return str(Path(p).relative_to(root))
        except ValueError:
            return p

    for key in ("py_path", "ipynb_path"):
        if key in result:
            result[key] = _rel(result[key])

    return result


def mcp_nb_lab(
    root: Path,
    flow: str | None = None,
    sync: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Build/refresh the Jupyter Lab symlink manifest.

    Creates ``.projio/pipeio/lab/<pipe>/<flow>/<name>.ipynb`` symlinks
    pointing to real notebook files.  Optionally syncs py→ipynb first.
    Returns manifest state (linked notebooks, stale cleaned, lab_dir).

    Args:
        root: Project root.
        flow: Filter to a specific flow (optional).
        sync: If True, sync py→ipynb before linking (default False).
        python_bin: Python binary where jupytext is installed (optional).
    """
    from pipeio.notebook.lifecycle import nb_lab

    result = nb_lab(
        root, flow=flow,
        sync=sync, python_bin=python_bin,
    )

    # Relativize target paths
    for item in result.get("linked", []):
        try:
            item["target"] = str(Path(item["target"]).relative_to(root))
        except ValueError:
            pass

    return result


def mcp_nb_scan(
    root: Path,
    register: bool = False,
) -> dict[str, Any]:
    """Scan for percent-format .py notebooks and compare against notebook.yml.

    Discovers .py files with ``# %%`` cell markers in ``notebooks/`` directories
    and reports which are registered vs unregistered.

    Args:
        root: Project root.
        register: If True, auto-register unregistered notebooks into
            notebook.yml with defaults (pair_ipynb=True, status=draft).
    """
    from pipeio.notebook.lifecycle import nb_scan

    results = nb_scan(root, register=register)

    # Relativize paths
    for item in results:
        for key in ("py_path", "flow_root"):
            if key in item:
                try:
                    item[key] = str(Path(item[key]).relative_to(root))
                except ValueError:
                    pass

    registered = [r for r in results if r["registered"]]
    unregistered = [r for r in results if not r["registered"]]

    return {
        "total": len(results),
        "registered": len(registered),
        "unregistered": len(unregistered),
        "notebooks": results,
    }


def mcp_nb_read(
    root: Path,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Read a notebook's .py content and return it with metadata.

    Combines file content, sync state, structural analysis, and config
    metadata (status, kernel, mod) in a single call.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return {"error": f"Notebook not found: {name}"}

    from pipeio.notebook.lifecycle import nb_read

    result = nb_read(py_path)

    # Enrich with config metadata from notebook.yml
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            from pipeio.notebook.config import NotebookConfig
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for nb in nb_cfg.entries:
                if Path(nb.path).stem == name:
                    result["status"] = nb.status
                    result["kind"] = nb.kind
                    result["mod"] = nb.mod
                    result["kernel"] = nb_cfg.resolve_kernel(nb)
                    result["description"] = nb.description
                    break
        except Exception:
            pass

    # Relativize paths
    for key in ("path",):
        if key in result:
            try:
                result[key] = str(Path(result[key]).relative_to(root))
            except ValueError:
                pass
    if "sync" in result:
        for k in ("py_path", "ipynb_path"):
            if k in result["sync"]:
                try:
                    result["sync"][k] = str(Path(result["sync"][k]).relative_to(root))
                except ValueError:
                    pass

    return result


def mcp_nb_audit(root: Path) -> dict[str, Any]:
    """Audit all notebooks: staleness, config completeness, mod coverage.

    Returns a holistic quality report with per-notebook issues and
    flow-level mod coverage gaps.

    Args:
        root: Project root.
    """
    from pipeio.notebook.lifecycle import nb_audit

    records = nb_audit(root)

    # Relativize flow_root
    for rec in records:
        if "flow_root" in rec:
            try:
                rec["flow_root"] = str(Path(rec["flow_root"]).relative_to(root))
            except ValueError:
                pass

    total_issues = sum(r.get("issue_count", 0) for r in records)
    notebooks = [r for r in records if r.get("name") != "__flow_coverage__"]
    coverage = [r for r in records if r.get("name") == "__flow_coverage__"]

    return {
        "total_notebooks": len(notebooks),
        "total_issues": total_issues,
        "notebooks": notebooks,
        "coverage_gaps": coverage,
    }


def mcp_nb_publish(
    root: Path,
    flow: str,
    name: str,
    format: str = "",
) -> dict[str, Any]:
    """Publish a notebook to the docs tree.

    Publishes to ``docs/pipelines/<pipe>/<flow>/notebooks/``.
    Supports both MyST markdown and HTML output.

    When *format* is empty, publishes whatever the notebook.yml entry
    has enabled (publish_myst and/or publish_html).

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
        format: Force a specific format ('myst', 'html', or '' for auto).
    """
    import shutil

    from pipeio.notebook.config import NotebookConfig
    from pipeio.notebook.lifecycle import _nb_output_paths, _nbconvert_html
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Resolve notebook paths
    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        # Try to construct the path for output resolution
        py_path = flow_dir / "notebooks" / ".src" / f"{name}.py"

    ipynb_path, myst_path = _nb_output_paths(py_path)

    # Determine what to publish from notebook.yml config
    publish_myst = True
    publish_html = False
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for nb in nb_cfg.entries:
                if Path(nb.path).stem == name:
                    publish_myst = nb.publish_myst
                    publish_html = nb.publish_html
                    break
        except Exception:
            pass

    # Override with explicit format
    if format == "myst":
        publish_myst, publish_html = True, False
    elif format == "html":
        publish_myst, publish_html = False, True

    # Write to .build/ only — docs_collect handles the final copy to docs/
    build_dir = flow_dir / ".build" / "notebooks"
    build_dir.mkdir(parents=True, exist_ok=True)
    published: list[str] = []

    if publish_myst:
        if myst_path.exists():
            build_dest = build_dir / f"{name}.md"
            shutil.copy2(myst_path, build_dest)
            published.append(str(build_dest))
        else:
            return {
                "error": f"MyST file not found: {myst_path}",
                "hint": "Run pipeio_nb_sync first to generate the .md file.",
            }

    if publish_html:
        if ipynb_path.exists():
            build_dest = build_dir / f"{name}.html"
            _nbconvert_html(ipynb_path, build_dest)
            published.append(str(build_dest))
        else:
            return {
                "error": f"ipynb file not found: {ipynb_path}",
                "hint": "Run pipeio_nb_sync first to generate the .ipynb file.",
            }

    if not published:
        return {
            "error": "Nothing to publish (publish_myst and publish_html both disabled)",
            "hint": "Set publish_myst or publish_html in notebook.yml, or pass format='html'.",
        }

    return {
        "published": published,
        "flow": flow,
        "name": name,
        "hint": "Run pipeio_docs_collect to copy to docs/pipelines/.",
    }


# ---------------------------------------------------------------------------
# Snakefile rule parser
# ---------------------------------------------------------------------------

_RULE_BLOCK_RE = re.compile(r"^rule\s+(\w+)\s*:", re.MULTILINE)
_MOD_PREFIX_RE = re.compile(r"^([a-z][a-z0-9]*?)_")

_SECTION_KEYWORDS = frozenset([
    "input", "output", "params", "script", "resources", "benchmark",
    "log", "wildcard_constraints", "priority", "threads", "conda",
    "envmodules", "shadow", "retries", "cache", "run",
])


def _line_indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _split_depth0(text: str) -> list[str]:
    """Split *text* at commas at nesting depth 0."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch in "([{":
            depth += 1
            current.append(ch)
        elif ch in ")]}":
            depth = max(0, depth - 1)
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _find_eq_depth0(text: str) -> int:
    """Return index of first ``=`` at nesting depth 0, or -1."""
    depth = 0
    for i, ch in enumerate(text):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth = max(0, depth - 1)
        elif ch == "=" and depth == 0:
            return i
    return -1


def _parse_section_kvs(lines: list[str]) -> dict[str, str]:
    """Extract ``name=value`` pairs from section content lines."""
    raw = " ".join(line.strip() for line in lines if line.strip())
    target: dict[str, str] = {}
    for part in _split_depth0(raw):
        part = part.strip().rstrip(",")
        if not part:
            continue
        eq = _find_eq_depth0(part)
        if eq == -1:
            continue
        name = part[:eq].strip()
        value = part[eq + 1:].strip()
        if re.match(r"^[a-zA-Z_]\w*$", name):
            target[name] = value
    return target


def _parse_snakefile_rules(text: str) -> list[dict[str, Any]]:
    """Parse a Snakefile text and return structured rule metadata.

    Returns a list of dicts with keys: ``name``, ``input``, ``output``,
    ``params``, ``script``.  Values for ``input``/``output``/``params`` are
    dicts of ``{name: raw_expression_string}``.
    """
    lines = text.splitlines()
    n = len(lines)
    rules: list[dict[str, Any]] = []

    # Find rule start positions
    starts: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        m = re.match(r"^rule\s+(\w+)\s*:", line)
        if m:
            starts.append((i, m.group(1)))

    for r_idx, (start, rule_name) in enumerate(starts):
        end = starts[r_idx + 1][0] if r_idx + 1 < len(starts) else n
        block = lines[start + 1 : end]

        # Determine section indentation (first non-empty line)
        section_indent: int | None = None
        for bline in block:
            if bline.strip():
                section_indent = _line_indent(bline)
                break
        if section_indent is None:
            rules.append({"name": rule_name, "input": {}, "output": {}, "params": {}, "script": None})
            continue

        # Walk block, collecting sections
        sections: list[tuple[str, list[str]]] = []
        cur_section: str | None = None
        cur_lines: list[str] = []

        for bline in block:
            if not bline.strip():
                if cur_section is not None:
                    cur_lines.append(bline)
                continue
            ind = _line_indent(bline)
            stripped = bline.strip()
            sec_m = re.match(r"^(\w+):\s*$", stripped)
            if ind == section_indent and sec_m and sec_m.group(1).lower() in _SECTION_KEYWORDS:
                if cur_section is not None:
                    sections.append((cur_section, cur_lines))
                cur_section = sec_m.group(1).lower()
                cur_lines = []
            elif cur_section is not None:
                cur_lines.append(bline)

        if cur_section is not None:
            sections.append((cur_section, cur_lines))

        rule_info: dict[str, Any] = {
            "name": rule_name,
            "input": {},
            "output": {},
            "params": {},
            "script": None,
        }
        for sec_name, sec_lines in sections:
            if sec_name == "script":
                for sl in sec_lines:
                    val = sl.strip().strip("'\"")
                    if val:
                        rule_info["script"] = val
                        break
            elif sec_name in ("input", "output", "params"):
                rule_info[sec_name] = _parse_section_kvs(sec_lines)

        rules.append(rule_info)

    return rules


# ---------------------------------------------------------------------------
# MCP tools: rule introspection and scaffolding
# ---------------------------------------------------------------------------


def mcp_rule_list(root: Path, flow: str | None = None) -> dict[str, Any]:
    """List rules for a flow with input/output signatures and mod membership.

    Parses the flow's Snakefile (and any ``.smk`` includes) and returns
    structured metadata for each rule: name, input/output/params dicts of
    ``{name: raw_expression}``, script path, and which mod the rule belongs to.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Build rule→mod lookup from registry
    rule_to_mod: dict[str, str] = {}
    for mod_name, mod_entry in entry.mods.items():
        for rname in mod_entry.rules:
            rule_to_mod[rname] = mod_name

    # Parse Snakefile + .smk files
    candidates: list[Path] = list(flow_dir.glob("*.smk"))
    snakefile = flow_dir / "Snakefile"
    if snakefile.exists():
        candidates.insert(0, snakefile)

    all_rules: list[dict[str, Any]] = []
    for sf in candidates:
        try:
            text = sf.read_text(encoding="utf-8")
        except Exception:
            continue
        for rule_info in _parse_snakefile_rules(text):
            rname = rule_info["name"]
            if rname in rule_to_mod:
                mod = rule_to_mod[rname]
            else:
                m = _MOD_PREFIX_RE.match(rname)
                mod = m.group(1) if m else rname
            rule_info["mod"] = mod
            rule_info["source_file"] = sf.name
            all_rules.append(rule_info)

    return {
        "flow": entry.name,
        "flow": entry.name,
        "rule_count": len(all_rules),
        "rules": all_rules,
    }


def _bids_call(kwargs: dict[str, Any]) -> str:
    """Render a ``bids(...)`` call from keyword-argument dict."""
    parts: list[str] = []
    for k, v in kwargs.items():
        if isinstance(v, str) and not (
            v.startswith("config") or v.startswith("**") or v.startswith("{")
        ):
            parts.append(f'{k}="{v}"')
        else:
            parts.append(f"{k}={v}")
    return f"bids({', '.join(parts)})"


def _config_path_to_expr(config_path: str) -> str:
    """Convert ``'section.key'`` to ``config["section"]["key"]``.

    If *config_path* already looks like a Python expression (contains ``[``
    or ``(``), it is returned verbatim to avoid double-wrapping.
    """
    if "[" in config_path or "(" in config_path:
        return config_path
    expr = "config"
    for part in config_path.split("."):
        expr += f'["{part}"]'
    return expr


def _input_spec_to_expr(spec: Any) -> str:
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict):
        if "source_rule" in spec:
            member = spec.get("member", "")
            return f"rules.{spec['source_rule']}.output.{member}" if member else f"rules.{spec['source_rule']}.output[0]"
        return _bids_call(spec)
    return repr(spec)


def _output_spec_to_expr(spec: Any) -> str:
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict):
        return _bids_call(spec)
    return repr(spec)


def mcp_rule_stub(
    root: Path,
    flow: str | None,
    rule_name: str,
    inputs: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
    script: str | None = None,
) -> dict[str, Any]:
    """Generate a syntactically correct Snakemake rule stub from a contract spec.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        rule_name: Name for the new rule.
        inputs: ``{name: bids_pattern_str}`` or ``{name: {source_rule, member}}``.
        outputs: ``{name: bids_kwargs_dict}`` or ``{name: bids_pattern_str}``.
        params: ``{name: config_dot_path}`` e.g. ``{"ttl_freq": "ttl_removal.ttl_freq"}``.
        script: Relative path to the script file.

    Returns a dict with ``stub`` (the formatted rule text) for human review.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    lines: list[str] = [f"rule {rule_name}:"]

    if inputs:
        lines.append("    input:")
        for name, spec in inputs.items():
            lines.append(f"        {name}={_input_spec_to_expr(spec)},")

    if outputs:
        lines.append("    output:")
        for name, spec in outputs.items():
            lines.append(f"        {name}={_output_spec_to_expr(spec)},")

    if params:
        lines.append("    params:")
        for name, cfg_path in params.items():
            lines.append(f"        {name}={_config_path_to_expr(cfg_path)},")

    if script:
        lines.append("    script:")
        lines.append(f'        "{script}"')

    stub_text = "\n".join(lines)
    return {
        "rule_name": rule_name,
        "flow": entry.name,
        "flow": entry.name,
        "stub": stub_text,
    }


def mcp_rule_insert(
    root: Path,
    flow: str | None = None,
    rule_name: str = "",
    rule_text: str | None = None,
    target_file: str | None = None,
    after_rule: str | None = None,
    inputs: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
    script: str | None = None,
) -> dict[str, Any]:
    """Insert a Snakemake rule into the correct .smk or Snakefile.

    Either provide ``rule_text`` directly, or provide ``inputs``/``outputs``/
    ``params``/``script`` to generate the rule (same as ``rule_stub``).

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        rule_name: Name for the rule (required).
        rule_text: Pre-formatted rule text to insert. If omitted, generated
            from inputs/outputs/params/script.
        target_file: Which ``.smk`` or ``Snakefile`` to insert into (basename).
            If omitted, auto-selects by mod prefix or uses the main Snakefile.
        after_rule: Insert after this rule name. If omitted, appends at end.
        inputs: ``{name: bids_pattern}`` (for generation, ignored if rule_text given).
        outputs: ``{name: bids_kwargs_dict}`` (for generation).
        params: ``{name: config_dot_path}`` (for generation).
        script: Relative path to the script file (for generation).
    """
    from pipeio.registry import PipelineRegistry

    if not rule_name:
        return {"error": "rule_name is required"}

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Generate rule text if not provided
    if rule_text is None:
        stub_lines: list[str] = [f"rule {rule_name}:"]
        if inputs:
            stub_lines.append("    input:")
            for name, spec in inputs.items():
                stub_lines.append(f"        {name}={_input_spec_to_expr(spec)},")
        if outputs:
            stub_lines.append("    output:")
            for name, spec in outputs.items():
                stub_lines.append(f"        {name}={_output_spec_to_expr(spec)},")
        if params:
            stub_lines.append("    params:")
            for name, cfg_path in params.items():
                stub_lines.append(f"        {name}={_config_path_to_expr(cfg_path)},")
        if script:
            stub_lines.append("    script:")
            stub_lines.append(f'        "{script}"')
        rule_text = "\n".join(stub_lines)

    # Collect all Snakefiles and check for duplicate rule name
    candidates: list[Path] = list(flow_dir.glob("*.smk"))
    snakefile = flow_dir / "Snakefile"
    if snakefile.exists():
        candidates.insert(0, snakefile)

    existing_rules: dict[str, Path] = {}
    for sf in candidates:
        try:
            text = sf.read_text(encoding="utf-8")
        except Exception:
            continue
        for ri in _parse_snakefile_rules(text):
            existing_rules[ri["name"]] = sf

    if rule_name in existing_rules:
        return {
            "error": f"Rule {rule_name!r} already exists in "
            f"{existing_rules[rule_name].name}",
        }

    # Determine target file
    if target_file:
        target_path = flow_dir / target_file
    else:
        # Auto-select: match mod prefix to existing .smk files
        m = _MOD_PREFIX_RE.match(rule_name)
        prefix = m.group(1) if m else None
        target_path = None
        if prefix:
            for sf in candidates:
                if sf.suffix == ".smk" and sf.stem == prefix:
                    target_path = sf
                    break
        if target_path is None:
            target_path = snakefile if snakefile.exists() else flow_dir / "Snakefile"

    # Read existing content (or start empty)
    if target_path.exists():
        original = target_path.read_text(encoding="utf-8")
    else:
        original = ""

    # Insert after a specific rule, or append
    if after_rule and original:
        file_lines = original.splitlines(keepends=True)
        # Find the end of after_rule block
        insert_idx = None
        in_target = False
        target_indent: int | None = None
        for i, line in enumerate(file_lines):
            rule_m = re.match(r"^rule\s+(\w+)\s*:", line)
            if rule_m and rule_m.group(1) == after_rule:
                in_target = True
                continue
            if in_target:
                # We're past the rule header; find where it ends
                if rule_m:
                    # Hit next rule — insert before this line
                    insert_idx = i
                    break
                # Also break on non-indented non-empty non-comment lines
                stripped = line.strip()
                if stripped and not stripped.startswith("#") and _line_indent(line) == 0:
                    insert_idx = i
                    break
        if insert_idx is None and in_target:
            insert_idx = len(file_lines)
        if insert_idx is None:
            return {"error": f"Rule {after_rule!r} not found in {target_path.name}"}

        # Insert with blank line separators
        insert_block = "\n\n" + rule_text + "\n"
        file_lines.insert(insert_idx, insert_block)
        new_content = "".join(file_lines)
    else:
        # Append at end
        sep = "\n\n" if original and not original.endswith("\n\n") else (
            "\n" if original and not original.endswith("\n") else ""
        )
        new_content = original + sep + rule_text + "\n"

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(new_content, encoding="utf-8")

    try:
        rel_path = str(target_path.relative_to(root))
    except ValueError:
        rel_path = str(target_path)

    return {
        "inserted": True,
        "rule_name": rule_name,
        "target_file": rel_path,
        "flow": entry.name,
        "flow": entry.name,
        "rule_text": rule_text,
        "after_rule": after_rule,
    }


def _find_rule_span(lines: list[str], rule_name: str) -> tuple[int, int] | None:
    """Return (start, end) line indices for *rule_name* in *lines*.

    ``start`` is the ``rule X:`` line.  ``end`` is the index of the first
    line that belongs to the *next* top-level construct (another rule, or
    EOF).
    """
    start: int | None = None
    for i, line in enumerate(lines):
        m = re.match(r"^rule\s+(\w+)\s*:", line)
        if m:
            if m.group(1) == rule_name:
                start = i
            elif start is not None:
                # Hit next rule — previous rule ends here
                return (start, i)
    if start is not None:
        return (start, len(lines))
    return None


def _rebuild_rule_text(
    rule_name: str,
    sections: dict[str, dict[str, str]],
    script_val: str | None,
    indent: str = "    ",
    kv_indent: str = "        ",
) -> str:
    """Rebuild a rule from parsed sections."""
    lines = [f"rule {rule_name}:"]
    for sec_name in ("input", "output", "params"):
        kvs = sections.get(sec_name)
        if not kvs:
            continue
        lines.append(f"{indent}{sec_name}:")
        for name, expr in kvs.items():
            lines.append(f"{kv_indent}{name}={expr},")
    if script_val:
        lines.append(f"{indent}script:")
        lines.append(f'{kv_indent}"{script_val}"')
    return "\n".join(lines)


def mcp_rule_update(
    root: Path,
    flow: str | None = None,
    rule_name: str = "",
    add_inputs: dict[str, Any] | None = None,
    add_outputs: dict[str, Any] | None = None,
    add_params: dict[str, str] | None = None,
    set_script: str | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    """Patch an existing Snakemake rule by merging new sections.

    Adds new entries to ``input``, ``output``, or ``params`` sections without
    overwriting existing ones.  ``set_script`` replaces the script path.

    Returns a unified diff preview by default; set ``apply=True`` to write.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        rule_name: Name of the existing rule to patch.
        add_inputs: ``{name: spec}`` entries to add to the input section.
        add_outputs: ``{name: spec}`` entries to add to the output section.
        add_params: ``{name: config_dot_path}`` entries to add to params.
        set_script: New script path (replaces existing).
        apply: Write the patched file (default False).
    """
    import difflib
    from pipeio.registry import PipelineRegistry

    if not rule_name:
        return {"error": "rule_name is required"}

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Find which file contains the rule
    candidates: list[Path] = list(flow_dir.glob("*.smk"))
    snakefile = flow_dir / "Snakefile"
    if snakefile.exists():
        candidates.insert(0, snakefile)

    source_file: Path | None = None
    for sf in candidates:
        try:
            text = sf.read_text(encoding="utf-8")
        except Exception:
            continue
        for ri in _parse_snakefile_rules(text):
            if ri["name"] == rule_name:
                source_file = sf
                break
        if source_file:
            break

    if source_file is None:
        return {"error": f"Rule {rule_name!r} not found in any Snakefile"}

    original_text = source_file.read_text(encoding="utf-8")
    file_lines = original_text.splitlines()

    span = _find_rule_span(file_lines, rule_name)
    if span is None:
        return {"error": f"Rule {rule_name!r} span could not be determined"}

    start, end = span

    # Parse the existing rule
    rule_block = "\n".join(file_lines[start:end])
    parsed = _parse_snakefile_rules(rule_block)
    if not parsed:
        return {"error": f"Failed to parse rule {rule_name!r}"}

    rule_info = parsed[0]
    cur_input = dict(rule_info.get("input", {}))
    cur_output = dict(rule_info.get("output", {}))
    cur_params = dict(rule_info.get("params", {}))
    cur_script = rule_info.get("script")

    # Merge new entries (don't overwrite existing keys)
    conflicts: list[str] = []
    if add_inputs:
        for name, spec in add_inputs.items():
            if name in cur_input:
                conflicts.append(f"input.{name} already exists")
            else:
                cur_input[name] = _input_spec_to_expr(spec)
    if add_outputs:
        for name, spec in add_outputs.items():
            if name in cur_output:
                conflicts.append(f"output.{name} already exists")
            else:
                cur_output[name] = _output_spec_to_expr(spec)
    if add_params:
        for name, cfg_path in add_params.items():
            if name in cur_params:
                conflicts.append(f"params.{name} already exists")
            else:
                cur_params[name] = _config_path_to_expr(cfg_path)

    script_val = set_script if set_script is not None else cur_script

    # Rebuild the rule text
    sections: dict[str, dict[str, str]] = {}
    if cur_input:
        sections["input"] = cur_input
    if cur_output:
        sections["output"] = cur_output
    if cur_params:
        sections["params"] = cur_params

    new_rule = _rebuild_rule_text(rule_name, sections, script_val)

    # Replace in file
    new_lines = file_lines[:start] + new_rule.splitlines() + file_lines[end:]
    new_text = "\n".join(new_lines)
    if not new_text.endswith("\n"):
        new_text += "\n"

    try:
        rel_path = str(source_file.relative_to(root))
    except ValueError:
        rel_path = str(source_file)

    diff = "".join(difflib.unified_diff(
        original_text.splitlines(keepends=True),
        new_text.splitlines(keepends=True),
        fromfile=f"a/{rel_path}",
        tofile=f"b/{rel_path}",
    ))

    applied = False
    if apply:
        source_file.write_text(new_text, encoding="utf-8")
        applied = True

    return {
        "rule_name": rule_name,
        "source_file": rel_path,
        "flow": entry.name,
        "flow": entry.name,
        "diff": diff,
        "applied": applied,
        "conflicts": conflicts,
    }


# ---------------------------------------------------------------------------
# Snakebids config introspection and patching
# ---------------------------------------------------------------------------


def _has_yaml_anchors(text: str) -> bool:
    """Return True if *text* contains YAML anchor definitions (``&name``)."""
    import re
    return bool(re.search(r"&\w+", text))


def _render_bids_signature(
    group: dict[str, Any],
    pybids_inputs: dict[str, Any],
    member: dict[str, Any],
) -> str:
    """Render a ``bids(...)`` call signature for one registry member.

    The group's ``bids`` section provides shared kwargs (root, datatype).
    The member dict provides per-product kwargs (suffix, extension, desc, …).
    Wildcards are derived from the ``pybids_inputs`` entry named by
    ``base_input``.
    """
    bids_kwargs: dict[str, Any] = dict(group.get("bids") or {})
    for k, v in member.items():
        bids_kwargs[k] = v

    parts: list[str] = []
    for k, v in bids_kwargs.items():
        parts.append(f'{k}="{v}"' if isinstance(v, str) else f"{k}={v}")

    base_input = group.get("base_input", "")
    wildcards: list[str] = []
    if base_input and base_input in pybids_inputs:
        wildcards = pybids_inputs[base_input].get("wildcards") or []
    if wildcards:
        parts.append(f"**wildcards  # {', '.join(wildcards)}")

    return f"bids({', '.join(parts)})"


def _validate_registry_entry(
    group_name: str,
    group: Any,
    pybids_inputs: dict[str, Any],
) -> list[str]:
    """Validate one registry group dict. Returns a list of error strings."""
    errors: list[str] = []
    if not isinstance(group, dict):
        errors.append(f"Group {group_name!r}: must be a dict")
        return errors

    base_input = group.get("base_input", "")
    if base_input and pybids_inputs and base_input not in pybids_inputs:
        known = ", ".join(pybids_inputs.keys()) or "(none)"
        errors.append(
            f"Group {group_name!r}: base_input={base_input!r} not found in "
            f"pybids_inputs. Known: {known}"
        )

    members = group.get("members") or {}
    if not members:
        errors.append(f"Group {group_name!r}: no members defined")

    for member_name, member in members.items():
        if not isinstance(member, dict):
            errors.append(
                f"Group {group_name!r}, member {member_name!r}: must be a dict"
            )
            continue
        if not member.get("suffix"):
            errors.append(
                f"Group {group_name!r}, member {member_name!r}: missing 'suffix'"
            )
        if not member.get("extension"):
            errors.append(
                f"Group {group_name!r}, member {member_name!r}: missing 'extension'"
            )

    return errors


def mcp_config_read(root: Path, flow: str | None = None) -> dict[str, Any]:
    """Read and parse a flow's config.yml with anchor resolution and bids mapping.

    Returns the parsed config broken into logical sections:

    - ``pybids_inputs`` — input sources and their wildcard lists
    - ``registry`` — output groups with members (anchors already resolved by YAML loader)
    - ``member_sets`` — the ``_member_sets`` anchor library (anchor *definitions*)
    - ``params`` — all other top-level config keys
    - ``bids_signatures`` — computed ``bids(...)`` call per group+member
    - ``has_anchors`` — whether the raw file uses YAML anchors (for information)
    """
    import yaml
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    reg = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = reg.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if not entry.config_path:
        return {"error": f"No config_path registered for {entry.name}"}

    cfg_path = Path(entry.config_path)
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path
    if not cfg_path.exists():
        return {"error": f"Config not found: {cfg_path}"}

    try:
        raw_text = cfg_path.read_text(encoding="utf-8")
        config: dict[str, Any] = yaml.safe_load(raw_text) or {}
    except Exception as exc:
        return {"error": f"Failed to parse config.yml: {exc}"}

    pybids_inputs: dict[str, Any] = config.get("pybids_inputs") or {}
    registry_data: dict[str, Any] = config.get("registry") or {}
    member_sets: dict[str, Any] = config.get("_member_sets") or {}
    params = {
        k: v for k, v in config.items()
        if k not in ("pybids_inputs", "registry", "_member_sets")
    }

    bids_signatures: dict[str, dict[str, str]] = {}
    for group_name, group in registry_data.items():
        if not isinstance(group, dict):
            continue
        members = group.get("members") or {}
        group_sigs: dict[str, str] = {}
        for member_name, member in members.items():
            if isinstance(member, dict):
                group_sigs[member_name] = _render_bids_signature(
                    group, pybids_inputs, member
                )
        bids_signatures[group_name] = group_sigs

    # Resolve path patterns via PipelineContext for each group/member
    resolved_patterns: dict[str, dict[str, str]] = {}
    try:
        from pipeio.config import FlowConfig
        from pipeio.resolver import PipelineContext

        flow_config = FlowConfig.from_yaml(cfg_path)
        ctx = PipelineContext.from_config(flow_config, root)
        for group_name in ctx.groups():
            group_patterns: dict[str, str] = {}
            for member_name in ctx.products(group_name):
                group_patterns[member_name] = ctx.pattern(group_name, member_name)
            resolved_patterns[group_name] = group_patterns
    except Exception:
        pass  # non-fatal: bids_signatures still available

    try:
        rel_path = str(cfg_path.relative_to(root))
    except ValueError:
        rel_path = str(cfg_path)

    return {
        "flow": entry.name,
        "flow": entry.name,
        "config_path": rel_path,
        "has_anchors": _has_yaml_anchors(raw_text),
        "pybids_inputs": pybids_inputs,
        "registry": registry_data,
        "member_sets": member_sets,
        "params": params,
        "bids_signatures": bids_signatures,
        "resolved_patterns": resolved_patterns,
    }


def _ruamel_preserve_anchors(data: Any) -> None:
    """Mark all anchored nodes with ``always_dump=True``.

    ruamel.yaml drops anchors that have no corresponding alias after
    round-trip.  Walking the tree and setting ``always_dump`` on every
    anchor preserves them even when unreferenced.
    """
    from ruamel.yaml.comments import CommentedMap, CommentedSeq

    seen: set[int] = set()

    def _walk(node: Any) -> None:
        nid = id(node)
        if nid in seen:
            return
        seen.add(nid)
        if hasattr(node, "anchor") and node.anchor.value:
            node.anchor.always_dump = True
        if isinstance(node, CommentedMap):
            for v in node.values():
                _walk(v)
        elif isinstance(node, (CommentedSeq, list)):
            for item in node:
                _walk(item)

    _walk(data)


def _ruamel_dump_str(data: Any) -> str:
    """Serialize a ruamel.yaml object to string."""
    from io import StringIO

    from ruamel.yaml import YAML

    _ruamel_preserve_anchors(data)
    ryaml = YAML()
    ryaml.preserve_quotes = True  # type: ignore[assignment]
    buf = StringIO()
    ryaml.dump(data, buf)
    return buf.getvalue()


def _ruamel_deep_update(
    target: Any, source: dict[str, Any]
) -> None:
    """Recursively update *target* (a ruamel CommentedMap) from plain *source*.

    New keys are inserted; existing dict values are merged recursively;
    non-dict values are replaced.  Flow-style mappings in *source* are
    converted to ``CommentedMap`` with ``fa.set_flow_style()`` so round-trip
    serialization preserves them.
    """
    from ruamel.yaml.comments import CommentedMap

    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            _ruamel_deep_update(target[key], value)
        else:
            if isinstance(value, dict):
                cm = CommentedMap(value)
                # Preserve flow style for small member dicts (e.g. {suffix: ieeg, extension: .fif})
                if all(not isinstance(v, (dict, list)) for v in value.values()):
                    cm.fa.set_flow_style()
                else:
                    # Recurse into nested dicts to convert them too
                    for k, v in value.items():
                        if isinstance(v, dict):
                            inner = CommentedMap(v)
                            if all(not isinstance(iv, (dict, list)) for iv in v.values()):
                                inner.fa.set_flow_style()
                            cm[k] = inner
                target[key] = cm
            else:
                target[key] = value


def mcp_config_patch(
    root: Path,
    flow: str | None = None,
    registry_entry: dict[str, Any] | None = None,
    params_entry: dict[str, Any] | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    """Validate a config patch and optionally apply it to config.yml.

    Validates ``registry_entry`` against the snakebids schema and checks that
    every ``base_input`` references an existing ``pybids_inputs`` key.  Returns
    a unified diff for review; set ``apply=True`` to write the patched file.

    Uses ``ruamel.yaml`` round-trip mode to preserve comments, anchors/aliases,
    flow-style mappings, blank lines, and key ordering.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        registry_entry: ``{group_name: group_dict}`` to add/replace in ``registry:``.
        params_entry: ``{section: {key: value}}`` to add/update in top-level params.
        apply: Write the patched config to disk (default False).
    """
    import difflib

    import yaml
    from ruamel.yaml import YAML

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    reg = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = reg.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if not entry.config_path:
        return {"error": f"No config_path registered for {entry.name}"}

    cfg_path = Path(entry.config_path)
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path
    if not cfg_path.exists():
        return {"error": f"Config not found: {cfg_path}"}

    try:
        original_text = cfg_path.read_text(encoding="utf-8")
        # Use PyYAML for validation (resolves anchors for schema checking)
        config: dict[str, Any] = yaml.safe_load(original_text) or {}
    except Exception as exc:
        return {"error": f"Failed to parse config.yml: {exc}"}

    pybids_inputs: dict[str, Any] = config.get("pybids_inputs") or {}

    # Validate
    all_errors: list[str] = []
    if registry_entry:
        for group_name, group in registry_entry.items():
            all_errors.extend(
                _validate_registry_entry(group_name, group, pybids_inputs)
            )

    if all_errors:
        return {
            "valid": False,
            "errors": all_errors,
            "warnings": [],
            "diff": "",
            "applied": False,
        }

    # Round-trip load with ruamel to preserve formatting
    ryaml = YAML()
    ryaml.preserve_quotes = True  # type: ignore[assignment]
    try:
        patched = ryaml.load(original_text)
        if patched is None:
            from ruamel.yaml.comments import CommentedMap
            patched = CommentedMap()
    except Exception as exc:
        return {"error": f"Failed to round-trip parse config.yml: {exc}"}

    # Apply registry_entry into the round-trip structure
    if registry_entry:
        if "registry" not in patched:
            from ruamel.yaml.comments import CommentedMap
            patched["registry"] = CommentedMap()
        _ruamel_deep_update(patched["registry"], registry_entry)

    # Apply params_entry into the round-trip structure
    if params_entry:
        for section, values in params_entry.items():
            if section in patched and isinstance(patched[section], dict):
                _ruamel_deep_update(patched, {section: values})
            else:
                patched[section] = values

    try:
        patched_text = _ruamel_dump_str(patched)
    except Exception as exc:
        return {"error": f"Failed to serialize patched config: {exc}"}

    try:
        rel_path = str(cfg_path.relative_to(root))
    except ValueError:
        rel_path = str(cfg_path)

    diff_lines = list(difflib.unified_diff(
        original_text.splitlines(keepends=True),
        patched_text.splitlines(keepends=True),
        fromfile=f"a/{rel_path}",
        tofile=f"b/{rel_path}",
    ))
    diff = "".join(diff_lines)

    warnings: list[str] = []

    applied = False
    if apply:
        cfg_path.write_text(patched_text, encoding="utf-8")
        applied = True

    return {
        "valid": True,
        "errors": [],
        "warnings": warnings,
        "diff": diff,
        "applied": applied,
        "config_path": rel_path,
    }


def mcp_config_init(
    root: Path,
    flow: str | None = None,
    input_dir: str = "",
    output_dir: str = "",
    pybids_inputs: dict[str, Any] | None = None,
    registry_groups: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Scaffold a new flow's config.yml with pybids_inputs and registry structure.

    Creates a well-structured ``config.yml`` for a flow that doesn't have one
    yet.  Populates ``input_dir``, ``output_dir``, ``pybids_inputs`` (if
    provided), and an initial ``registry`` section.

    Use ``config_patch`` to modify an existing config.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        input_dir: Path to input data (relative to project root).
        output_dir: Path to output derivatives (relative to project root).
        pybids_inputs: Workflow-engine input spec (passed through to config).
        registry_groups: ``{group_name: group_dict}`` for the output registry.
        params: Additional top-level config parameters.
    """
    import yaml
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    reg = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = reg.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    # Resolve config path from registry entry or derive from code_path
    if entry.config_path:
        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
    else:
        flow_dir = Path(entry.code_path)
        if not flow_dir.is_absolute():
            flow_dir = root / flow_dir
        cfg_path = flow_dir / "config.yml"

    if cfg_path.exists():
        return {
            "error": f"Config already exists: {cfg_path.relative_to(root)}",
            "hint": "Use config_patch to modify an existing config.",
        }

    # Validate registry groups if provided
    warnings: list[str] = []
    if registry_groups:
        pybids = pybids_inputs or {}
        for group_name, group in registry_groups.items():
            errs = _validate_registry_entry(group_name, group, pybids)
            if errs:
                return {"valid": False, "errors": errs}

    # Build config dict
    config: dict[str, Any] = {}

    if input_dir:
        config["input_dir"] = input_dir
    if pybids_inputs:
        config["pybids_inputs"] = pybids_inputs

    # Output paths
    if not output_dir:
        output_dir = f"derivatives/{flow}"
    config["output_dir"] = output_dir
    config["output_manifest"] = (
        f"{output_dir}/manifest.yml"
    )

    # Registry
    config["registry"] = registry_groups or {}

    # Additional params
    if params:
        config.update(params)

    # Ensure parent directory exists
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    # Write YAML
    yaml_text = yaml.dump(
        config,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    cfg_path.write_text(yaml_text, encoding="utf-8")

    # Update registry entry's config_path if not set
    if not entry.config_path:
        try:
            rel_cfg = str(cfg_path.relative_to(root))
        except ValueError:
            rel_cfg = str(cfg_path)
        entry.config_path = rel_cfg
        reg.to_yaml(registry_path)

    try:
        rel_path = str(cfg_path.relative_to(root))
    except ValueError:
        rel_path = str(cfg_path)

    return {
        "created": rel_path,
        "flow": entry.name,
        "flow": entry.name,
        "output_dir": output_dir,
        "registry_groups": list((registry_groups or {}).keys()),
        "warnings": warnings,
        "preview": yaml_text,
    }


def mcp_registry_validate(root: Path) -> dict[str, Any]:
    """Validate pipeline registry consistency."""
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    result = registry.validate(root=root)

    flows = registry.list_flows()
    total_mods = sum(len(f.mods) for f in flows)

    return {
        "valid": result.ok,
        "errors": result.errors,
        "warnings": result.warnings,
        "stats": {
            "pipes": len(registry.list_flows()),
            "flows": len(flows),
            "mods": total_mods,
        },
    }


def mcp_nb_analyze(
    root: Path,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Analyze a notebook's static structure.

    Parses the percent-format ``.py`` file and returns structured metadata:
    imports, RunCard @dataclass fields, PipelineContext usage, section headers,
    and cogpy function calls.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
    """
    from pipeio.notebook.analyze import analyze_notebook
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return {"error": f"Notebook not found: {name}"}
    return analyze_notebook(py_path)


# ---------------------------------------------------------------------------
# MCP tools: mod scaffolding
# ---------------------------------------------------------------------------


def mcp_mod_create(
    root: Path,
    flow: str,
    mod: str,
    description: str = "",
    from_notebook: str | None = None,
    inputs: dict[str, str] | None = None,
    outputs: dict[str, str] | None = None,
    params_spec: dict[str, str] | None = None,
    use_pipeline_context: bool = False,
) -> dict[str, Any]:
    """Scaffold a new pipeline mod (script skeleton + doc stub).

    Creates ``scripts/<mod>.py`` with a processing template and
    ``docs/mod-<mod>.md`` with frontmatter.  Optionally seeds the script
    from a notebook's analyzed imports and function calls.

    When ``inputs``/``outputs``/``params_spec`` are provided, the generated
    script includes Snakemake I/O unpacking and parameter binding so that
    only the processing logic needs to be filled in.

    Args:
        root: Project root.
        flow: Flow name.
        mod: Mod name (lowercase, underscore-separated).
        description: One-line purpose for the mod header.
        from_notebook: Notebook name to seed imports from (optional).
        inputs: ``{var_name: description}`` for snakemake.input unpacking.
        outputs: ``{var_name: description}`` for snakemake.output unpacking.
        params_spec: ``{var_name: description}`` for snakemake.params unpacking.
        use_pipeline_context: Generate PipelineContext setup boilerplate.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Validate mod name
    if not re.match(r"^[a-z][a-z0-9_]*$", mod):
        return {"error": f"Invalid mod name: {mod!r}. Use lowercase + underscores."}

    # Create scripts directory and mod script
    scripts_dir = flow_dir / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    script_path = scripts_dir / f"{mod}.py"
    if script_path.exists():
        return {"error": f"Script already exists: {script_path.relative_to(root)}"}

    # Seed imports from notebook if requested
    nb_imports: list[str] = []
    if from_notebook:
        try:
            from pipeio.notebook.analyze import analyze_notebook
            nb_path = flow_dir / "notebooks" / f"{from_notebook}.py"
            if nb_path.exists():
                analysis = analyze_notebook(nb_path)
                for imp in analysis.get("imports", []):
                    if imp.get("kind") == "import":
                        line = f"import {imp['module']}"
                        if imp.get("alias"):
                            line += f" as {imp['alias']}"
                        nb_imports.append(line)
                    elif imp.get("kind") == "from":
                        names = ", ".join(imp.get("names", []))
                        nb_imports.append(f"from {imp['module']} import {names}")
        except Exception:
            pass

    has_io = bool(inputs or outputs or params_spec)

    # Generate script content
    lines: list[str] = []
    desc_text = description or f"Processing script for mod: {mod}"
    lines.append(f'"""')
    lines.append(f"{desc_text}")
    lines.append(f'"""')
    lines.append("")
    lines.append("from pathlib import Path")
    lines.append("")
    if use_pipeline_context:
        lines.append("from pipeio.resolver import PipelineContext")
        lines.append("")
    if nb_imports:
        lines.extend(nb_imports)
        lines.append("")
    lines.append("")
    lines.append("def main(snakemake):")
    lines.append(f'    """Entry point called by Snakemake rule."""')

    if has_io or use_pipeline_context:
        # --- I/O unpacking ---
        if inputs:
            lines.append("")
            lines.append("    # --- Inputs ---")
            for var, desc in inputs.items():
                lines.append(f"    {var} = Path(snakemake.input.{var})  # {desc}")

        if outputs:
            lines.append("")
            lines.append("    # --- Outputs ---")
            for var, desc in outputs.items():
                lines.append(f"    {var} = Path(snakemake.output.{var})  # {desc}")

        if params_spec:
            lines.append("")
            lines.append("    # --- Parameters ---")
            for var, desc in params_spec.items():
                lines.append(f"    {var} = snakemake.params.{var}  # {desc}")

        if use_pipeline_context:
            lines.append("")
            lines.append("    # --- Pipeline context ---")
            lines.append(f'    ctx = PipelineContext.from_config(Path(snakemake.params.config_path))')
            lines.append("    # session = ctx.session(sub=..., ses=...)")

        lines.append("")
        lines.append("    # --- Processing (TODO: implement) ---")
        if outputs:
            lines.append("")
            first_out = next(iter(outputs))
            lines.append(f"    {first_out}.parent.mkdir(parents=True, exist_ok=True)")
        lines.append("    pass")
    else:
        lines.append("    pass")
    lines.append("")
    lines.append("")
    lines.append('if __name__ == "__main__":')
    lines.append("    main(snakemake)  # noqa: F821")
    lines.append("")

    script_path.write_text("\n".join(lines), encoding="utf-8")

    # Create faceted doc stubs: docs/{mod}/theory.md + spec.md
    mod_docs_dir = flow_dir / "docs" / mod
    mod_docs_dir.mkdir(parents=True, exist_ok=True)

    created_docs: list[str] = []

    theory_path = mod_docs_dir / "theory.md"
    if not theory_path.exists():
        theory_lines = [
            "---",
            f"mod: {mod}",
            f"flow: {entry.name}",
            "facet: theory",
            "---",
            "",
            f"# {mod.replace('_', ' ').title()}",
            "",
            desc_text,
            "",
            "## Rationale",
            "",
            "<!-- Scientific rationale and method justification. -->",
            "<!-- Use pandoc citations: [@citekey] -->",
            "",
            "## References",
            "",
        ]
        theory_path.write_text("\n".join(theory_lines), encoding="utf-8")
        created_docs.append("theory.md")

    spec_path = mod_docs_dir / "spec.md"
    if not spec_path.exists():
        spec_lines = [
            "---",
            f"mod: {mod}",
            f"flow: {entry.name}",
            "facet: spec",
            "---",
            "",
            f"# {mod.replace('_', ' ').title()} — Specification",
            "",
            "## I/O Contract",
            "",
            "<!-- Input/output paths, BIDS entities, file formats. -->",
            "",
            "## Parameters",
            "",
            "<!-- Config parameters and their meaning. -->",
            "",
            "## Components",
            "",
            "<!-- Rules, scripts, and their relationships. -->",
            "",
        ]
        spec_path.write_text("\n".join(spec_lines), encoding="utf-8")
        created_docs.append("spec.md")

    try:
        script_rel = str(script_path.relative_to(root))
        doc_rel = str(mod_docs_dir.relative_to(root))
    except ValueError:
        script_rel = str(script_path)
        doc_rel = str(mod_docs_dir)

    return {
        "created_script": script_rel,
        "created_docs": created_docs,
        "doc_dir": doc_rel,
        "flow": entry.name,
        "mod": mod,
        "seeded_from": from_notebook if nb_imports else None,
        "io_wiring": has_io,
        "pipeline_context": use_pipeline_context,
    }


# ---------------------------------------------------------------------------
# MCP tools: mod audit
# ---------------------------------------------------------------------------


def mcp_mod_audit(
    root: Path,
    flow: str | None = None,
    mod: str = "",
) -> dict[str, Any]:
    """Audit a mod's health: contract drift, doc/script existence, naming.

    Returns structured findings without modifying anything.  Checks:

    1. **Script existence** — every ``script:`` reference resolves to a file.
    2. **Contract drift** — Snakefile I/O entries have matching config registry
       groups/members.
    3. **Doc coverage** — ``docs/{mod}/theory.md`` and ``spec.md`` exist and
       are non-empty.
    4. **Naming convention** — all rules follow the ``{mod}_*`` prefix.
    5. **Registry consistency** — registered mod rules match rules discovered
       in the Snakefile.

    Args:
        root: Project root.
        flow: Flow name (required unless single-flow project).
        mod: Mod name (audits all mods in the flow if empty).
    """
    import re

    import yaml
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Parse all rules from Snakefile + *.smk
    all_rules: list[dict[str, Any]] = []
    candidates: list[Path] = list(flow_dir.glob("rules/*.smk"))
    snakefile = flow_dir / "Snakefile"
    if snakefile.exists():
        candidates.insert(0, snakefile)
    for sf in candidates:
        try:
            text = sf.read_text(encoding="utf-8")
        except Exception:
            continue
        for rule_info in _parse_snakefile_rules(text):
            rule_info["source_file"] = sf.name
            all_rules.append(rule_info)

    # Build rule → mod mapping from registry
    reg_rule_to_mod: dict[str, str] = {}
    for mname, me in entry.mods.items():
        for rname in me.rules:
            reg_rule_to_mod[rname] = mname

    # Load config for registry group cross-checking
    config_groups: set[str] = set()
    if entry.config_path:
        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        if cfg_path.exists():
            try:
                raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
                config_groups = set(raw.get("registry", {}).keys())
            except Exception:
                pass

    # Determine which mods to audit
    target_mods: set[str]
    if mod:
        if mod not in entry.mods:
            return {"error": f"Mod {mod!r} not found in {entry.name}"}
        target_mods = {mod}
    else:
        target_mods = set(entry.mods.keys())

    mod_reports: list[dict[str, Any]] = []

    for mod_name in sorted(target_mods):
        mod_entry = entry.mods[mod_name]
        findings: list[dict[str, str]] = []

        # --- 1. Registry consistency: registered rules vs discovered ---
        registered_rules = set(mod_entry.rules)
        # Rules that match the mod prefix in the Snakefile
        discovered_rules = {
            r["name"] for r in all_rules
            if r["name"].startswith(f"{mod_name}_") or r["name"] == mod_name
        }
        missing_in_snakefile = registered_rules - {r["name"] for r in all_rules}
        unregistered = discovered_rules - registered_rules

        for rname in sorted(missing_in_snakefile):
            findings.append({
                "severity": "error",
                "check": "registry_consistency",
                "message": f"Rule {rname!r} registered but not found in Snakefile",
            })
        for rname in sorted(unregistered):
            findings.append({
                "severity": "warning",
                "check": "registry_consistency",
                "message": f"Rule {rname!r} matches mod prefix but not registered",
            })

        # --- 2. Naming convention ---
        for rname in registered_rules:
            if not rname.startswith(f"{mod_name}_") and rname != mod_name:
                findings.append({
                    "severity": "warning",
                    "check": "naming",
                    "message": f"Rule {rname!r} doesn't follow {mod_name}_* prefix",
                })

        # --- 3. Script existence ---
        mod_rules = [r for r in all_rules if r["name"] in registered_rules]
        for rule in mod_rules:
            script = rule.get("script")
            if script:
                script_path = flow_dir / script
                if not script_path.exists():
                    findings.append({
                        "severity": "error",
                        "check": "script_exists",
                        "message": f"Script {script!r} not found (rule {rule['name']})",
                    })

        # --- 4. Contract drift: check output groups reference config registry ---
        for rule in mod_rules:
            for out_name, out_expr in (rule.get("output") or {}).items():
                out_str = str(out_expr)
                # Look for out_paths("group", ...) or bids(root="group", ...)
                group_match = re.search(
                    r'(?:out_paths|bids)\s*\(\s*["\'](\w+)["\']', out_str
                )
                if group_match:
                    group = group_match.group(1)
                    if config_groups and group not in config_groups:
                        findings.append({
                            "severity": "warning",
                            "check": "contract_drift",
                            "message": (
                                f"Rule {rule['name']} output references group "
                                f"{group!r} not in config registry"
                            ),
                        })

        # --- 5. Doc coverage ---
        docs_dir = flow_dir / "docs" / mod_name
        for facet in ("theory.md", "spec.md"):
            facet_path = docs_dir / facet
            if not facet_path.exists():
                findings.append({
                    "severity": "info",
                    "check": "doc_coverage",
                    "message": f"Missing docs/{mod_name}/{facet}",
                })
            elif facet_path.stat().st_size < 50:
                findings.append({
                    "severity": "info",
                    "check": "doc_coverage",
                    "message": f"docs/{mod_name}/{facet} appears to be a stub (<50 bytes)",
                })

        errors = [f for f in findings if f["severity"] == "error"]
        warnings = [f for f in findings if f["severity"] == "warning"]
        infos = [f for f in findings if f["severity"] == "info"]

        mod_reports.append({
            "mod": mod_name,
            "rules_registered": len(registered_rules),
            "rules_discovered": len(discovered_rules),
            "findings": findings,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "info_count": len(infos),
        })

    total_errors = sum(r["error_count"] for r in mod_reports)
    total_warnings = sum(r["warning_count"] for r in mod_reports)

    return {
        "flow": entry.name,
        "mods_audited": len(mod_reports),
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "healthy": total_errors == 0,
        "mods": mod_reports,
    }


# ---------------------------------------------------------------------------
# MCP tools: mod doc refresh + script create
# ---------------------------------------------------------------------------


def mcp_mod_doc_refresh(
    root: Path,
    flow: str,
    mod: str,
    facet: str = "spec",
    apply: bool = False,
) -> dict[str, Any]:
    """Regenerate a mod doc facet from current mod_context.

    Generates an updated ``spec.md`` (default) or ``theory.md`` skeleton
    from the mod's current rules, scripts, config params, and bids
    signatures.  Returns the content as a preview; set ``apply=True``
    to write to disk.

    Args:
        root: Project root.
        flow: Flow name.
        mod: Mod name.
        facet: Which facet to refresh (``spec`` or ``theory``).
        apply: Write the refreshed doc to disk (default False).
    """
    if facet not in ("spec", "theory"):
        return {"error": f"facet must be 'spec' or 'theory', got {facet!r}"}

    # Get current mod context
    ctx = mcp_mod_context(root, flow=flow, mod=mod)
    if "error" in ctx:
        return ctx

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY
    registry = PipelineRegistry.from_yaml(registry_path)
    entry = registry.get(flow)

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    doc_dir = flow_dir / "docs" / mod
    doc_path = doc_dir / f"{facet}.md"

    if facet == "spec":
        lines = [
            "---",
            f"mod: {mod}",
            f"flow: {entry.name}",
            "facet: spec",
            "---",
            "",
            f"# {mod.replace('_', ' ').title()} — Specification",
            "",
            "## Rules",
            "",
        ]
        for rule in ctx.get("rules", []):
            script = rule.get("script", "")
            script_info = f" (`{script}`)" if script else ""
            lines.append(f"- **{rule['name']}**{script_info}")
            for io_type in ("input", "output"):
                io_dict = rule.get(io_type) or {}
                if io_dict:
                    lines.append(f"  - {io_type}: {', '.join(io_dict.keys())}")
        lines.append("")

        lines.append("## I/O Contract")
        lines.append("")
        sigs = ctx.get("bids_signatures") or {}
        if sigs:
            lines.append("| Group | Member | bids() signature |")
            lines.append("|-------|--------|-----------------|")
            for group, members in sigs.items():
                for member, sig in members.items():
                    lines.append(f"| {group} | {member} | `{sig}` |")
            lines.append("")
        else:
            lines.append("No bids signatures found.")
            lines.append("")

        lines.append("## Parameters")
        lines.append("")
        params = ctx.get("config_params") or {}
        if params:
            for section, values in params.items():
                lines.append(f"### {section}")
                lines.append("")
                if isinstance(values, dict):
                    for k, v in values.items():
                        lines.append(f"- `{k}`: `{v}`")
                else:
                    lines.append(f"- `{values}`")
                lines.append("")
        else:
            lines.append("No config parameters referenced.")
            lines.append("")

        lines.append("## Scripts")
        lines.append("")
        for script_path, _content in (ctx.get("scripts") or {}).items():
            lines.append(f"- `{script_path}`")
        lines.append("")

    else:  # theory
        lines = [
            "---",
            f"mod: {mod}",
            f"flow: {entry.name}",
            "facet: theory",
            "---",
            "",
            f"# {mod.replace('_', ' ').title()}",
            "",
            "## Rationale",
            "",
            "<!-- Scientific rationale and method justification. -->",
            "<!-- Use pandoc citations: [@citekey] -->",
            "",
            "## Method",
            "",
            f"This mod contains {len(ctx.get('rules', []))} rule(s): "
            f"{', '.join(r['name'] for r in ctx.get('rules', []))}.",
            "",
            "## References",
            "",
        ]

    content = "\n".join(lines)

    if apply:
        doc_dir.mkdir(parents=True, exist_ok=True)
        doc_path.write_text(content, encoding="utf-8")

    try:
        rel = str(doc_path.relative_to(root))
    except ValueError:
        rel = str(doc_path)

    return {
        "facet": facet,
        "path": rel,
        "content": content,
        "applied": apply,
        "exists": doc_path.exists(),
    }


def mcp_script_create(
    root: Path,
    flow: str,
    mod: str,
    script_name: str,
    description: str = "",
    inputs: dict[str, str] | None = None,
    outputs: dict[str, str] | None = None,
    params_spec: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Create an additional script for an existing mod.

    Use this when a mod already exists (via ``mod_create``) and you need
    a second or third script for additional rules.  Generates the same
    Snakemake-compatible script template as ``mod_create``.

    Args:
        root: Project root.
        flow: Flow name.
        mod: Mod name (for context — used in docstring, not in path).
        script_name: Script filename (without .py extension).
        description: One-line purpose.
        inputs: ``{var_name: description}`` for snakemake.input unpacking.
        outputs: ``{var_name: description}`` for snakemake.output unpacking.
        params_spec: ``{var_name: description}`` for snakemake.params unpacking.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    scripts_dir = flow_dir / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    # Clean up script name
    if script_name.endswith(".py"):
        script_name = script_name[:-3]

    script_path = scripts_dir / f"{script_name}.py"
    if script_path.exists():
        return {"error": f"Script already exists: {script_path.relative_to(root)}"}

    # Discover compute library via codio
    compute_lib = ""
    try:
        from codio import load_config as codio_load_config  # type: ignore[import]
        from codio import Registry as CodioRegistry  # type: ignore[import]
        codio_cfg = codio_load_config(root)
        codio_reg = CodioRegistry.load(codio_cfg)
        internals = [lib for lib in codio_reg.list() if lib.kind == "internal"]
        if internals:
            compute_lib = internals[0].runtime_import or internals[0].name
    except Exception:
        pass

    has_io = bool(inputs or outputs or params_spec)
    desc_text = description or f"Script for mod {mod}: {script_name}"

    lines: list[str] = [
        f'"""{desc_text}"""',
        "",
        "from pathlib import Path",
        "",
    ]
    if compute_lib:
        lines.append(f"import {compute_lib}")
        lines.append("")

    lines.extend([
        "",
        "def main(snakemake):",
        '    """Entry point called by Snakemake rule."""',
    ])

    if has_io:
        if inputs:
            lines.append("")
            lines.append("    # --- Inputs ---")
            for var, desc in inputs.items():
                lines.append(f"    {var} = Path(snakemake.input.{var})  # {desc}")

        if outputs:
            lines.append("")
            lines.append("    # --- Outputs ---")
            for var, desc in outputs.items():
                lines.append(f"    {var} = Path(snakemake.output.{var})  # {desc}")

        if params_spec:
            lines.append("")
            lines.append("    # --- Parameters ---")
            for var, desc in params_spec.items():
                lines.append(f"    {var} = snakemake.params.{var}  # {desc}")

        lines.append("")
        lines.append("    # --- Processing ---")
        if outputs:
            first_out = next(iter(outputs))
            lines.append(f"    {first_out}.parent.mkdir(parents=True, exist_ok=True)")
        lines.append("    pass")
    else:
        lines.append("    pass")

    lines.extend([
        "",
        "",
        'if __name__ == "__main__":',
        "    main(snakemake)  # noqa: F821",
        "",
    ])

    script_path.write_text("\n".join(lines), encoding="utf-8")

    try:
        rel = str(script_path.relative_to(root))
    except ValueError:
        rel = str(script_path)

    return {
        "created": rel,
        "flow": entry.name,
        "mod": mod,
        "script_name": f"{script_name}.py",
        "compute_library": compute_lib or None,
        "io_wiring": has_io,
    }


# ---------------------------------------------------------------------------
# MCP tools: notebook promotion
# ---------------------------------------------------------------------------


def mcp_nb_promote(
    root: Path,
    flow: str,
    name: str,
    mod: str,
    rule_name: str = "",
    description: str = "",
    apply: bool = False,
) -> dict[str, Any]:
    """Promote a notebook to a pipeline mod: analyze → script → rule → config → docs.

    Orchestrates the full notebook-to-mod extraction pipeline:

    1. Analyze the notebook (imports, sections, processing logic)
    2. Generate a script skeleton from the notebook's core cells
    3. Generate a Snakemake rule stub
    4. Scaffold mod doc stubs (theory.md + spec.md)
    5. Return everything as a preview bundle

    By default, only the script is written. Rule stubs and config patches
    are returned for review. Set ``apply=True`` to also create docs.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
        mod: Target mod name.
        rule_name: Rule name (default: same as mod).
        description: One-line purpose for the mod.
        apply: If True, also create doc stubs on disk.
    """
    from pipeio.notebook.analyze import analyze_notebook
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    if not rule_name:
        rule_name = mod

    # --- Step 1: Analyze notebook ---
    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return {"error": f"Notebook not found: {name}"}

    analysis = analyze_notebook(py_path)

    # Extract imports for the script
    import_lines: list[str] = []
    for imp in analysis.get("imports", []):
        if imp.get("kind") == "import":
            line = f"import {imp['module']}"
            if imp.get("alias"):
                line += f" as {imp['alias']}"
            import_lines.append(line)
        elif imp.get("kind") == "from":
            names = ", ".join(imp.get("names", []))
            import_lines.append(f"from {imp['module']} import {names}")

    # --- Step 2: Generate script skeleton ---
    script_name = f"{mod}.py"
    script_path = flow_dir / "scripts" / script_name

    desc_text = description or f"Processing script for mod: {mod}"
    script_lines: list[str] = [
        f'"""{desc_text}',
        f"",
        f"Promoted from notebook: {name}",
        f'"""',
        "",
        "from pathlib import Path",
        "",
    ]
    if import_lines:
        script_lines.extend(import_lines)
        script_lines.append("")

    script_lines.extend([
        "",
        "def main(snakemake):",
        '    """Entry point called by Snakemake rule."""',
        "",
        "    # --- Inputs ---",
        "    # TODO: unpack snakemake.input",
        "",
        "    # --- Outputs ---",
        "    # TODO: unpack snakemake.output",
        "",
        "    # --- Processing ---",
        "    # TODO: extract core logic from notebook",
        "    pass",
        "",
        "",
        'if __name__ == "__main__":',
        "    main(snakemake)  # noqa: F821",
        "",
    ])

    script_content = "\n".join(script_lines)
    script_exists = script_path.exists()

    if not script_exists:
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text(script_content, encoding="utf-8")

    # --- Step 3: Generate rule stub ---
    rule_stub = (
        f"rule {rule_name}:\n"
        f"    input:\n"
        f"        # TODO: define inputs\n"
        f"    output:\n"
        f"        # TODO: define outputs\n"
        f"    script:\n"
        f'        "scripts/{script_name}"\n'
    )

    # --- Step 4: Scaffold mod docs (if apply=True) ---
    docs_created: list[str] = []
    if apply:
        result = mcp_mod_create(
            root, flow=flow, mod=mod,
            description=desc_text, from_notebook=name,
        )
        if "error" not in result:
            docs_created = result.get("created_docs", [])

    # --- Step 5: Build result ---
    try:
        script_rel = str(script_path.relative_to(root))
    except ValueError:
        script_rel = str(script_path)

    return {
        "flow": flow,
        "notebook": name,
        "mod": mod,
        "rule_name": rule_name,
        "analysis": {
            "sections": analysis.get("sections", []),
            "imports": len(analysis.get("imports", [])),
            "cells": analysis.get("cell_count", 0),
        },
        "script": {
            "path": script_rel,
            "created": not script_exists,
            "exists": script_exists,
        },
        "rule_stub": rule_stub,
        "docs_created": docs_created,
        "next_steps": [
            f"Edit {script_rel}: extract core processing logic from notebook",
            f"Review rule stub and insert into Snakefile via pipeio_rule_insert",
            f"Add output registry group via pipeio_config_patch",
            f"Fill in docs/{mod}/theory.md with scientific rationale",
            f"Fill in docs/{mod}/spec.md with I/O contracts",
        ],
    }


# ---------------------------------------------------------------------------
# MCP tools: notebook report extraction
# ---------------------------------------------------------------------------


def _nb_report_percent(
    root: Path,
    flow_name: str,
    flow_dir: Path,
    py_path: Path,
    name: str,
    *,
    overwrite: bool,
    tags_only: bool,
    kernel: str,
) -> dict[str, Any]:
    """Extract report payload from a percent-format (ipynb) notebook."""
    from pipeio.notebook.backend import resolve_backend

    backend = resolve_backend("percent", py_path)
    outputs = backend.output_paths(py_path)
    ipynb_path = outputs.get("ipynb")

    if ipynb_path is None or not ipynb_path.exists():
        return {
            "error": (
                f"Notebook .ipynb not found for {name}. "
                "Sync with pipeio_nb_sync first."
            ),
        }

    # Check notebook has been executed (at least one cell with outputs)
    import json as _json

    nb_data = _json.loads(ipynb_path.read_text(encoding="utf-8"))
    has_outputs = any(
        cell.get("outputs")
        for cell in nb_data.get("cells", [])
        if cell.get("cell_type") == "code"
    )
    if not has_outputs:
        return {
            "error": (
                "Notebook has not been executed — no cell outputs found. "
                "Run pipeio_nb_exec first."
            ),
        }

    # Extract via nbconvert
    try:
        from nbconvert import MarkdownExporter
        from nbconvert.preprocessors import ExtractOutputPreprocessor
    except ImportError:
        return {
            "error": "nbconvert required — install via: pip install nbconvert",
        }

    exporter = MarkdownExporter()
    exporter.register_preprocessor(ExtractOutputPreprocessor, enabled=True)
    _body, resources = exporter.from_filename(str(ipynb_path))

    # Classify cells from the raw notebook JSON
    markdown_cells: list[dict[str, Any]] = []
    figures: list[dict[str, Any]] = []
    text_outputs: list[dict[str, Any]] = []
    html_outputs: list[dict[str, Any]] = []

    _REPORT_MD_TAG = "# REPORT:"
    _REPORT_CODE_TAG = "# REPORT"

    for cell_idx, cell in enumerate(nb_data.get("cells", [])):
        cell_type = cell.get("cell_type", "")
        source = "".join(cell.get("source", []))

        if tags_only:
            if cell_type == "markdown" and not source.lstrip().startswith(_REPORT_MD_TAG):
                continue
            if cell_type == "code":
                first_lines = source.split("\n", 3)[:3]
                if not any(line.strip() == _REPORT_CODE_TAG for line in first_lines):
                    continue

        if cell_type == "markdown":
            content = source
            if tags_only and content.lstrip().startswith(_REPORT_MD_TAG):
                # Strip the tag prefix from the first line
                lines = content.split("\n", 1)
                first = lines[0].replace(_REPORT_MD_TAG, "", 1).strip()
                rest = lines[1] if len(lines) > 1 else ""
                content = f"# {first}\n{rest}" if first else rest
            markdown_cells.append({"cell_index": cell_idx, "content": content})

        elif cell_type == "code":
            for output in cell.get("outputs", []):
                output_type = output.get("output_type", "")
                data = output.get("data", {})

                # Text outputs (stream + execute_result text)
                if output_type == "stream":
                    text = "".join(output.get("text", []))
                    if text.strip():
                        text_outputs.append({
                            "cell_index": cell_idx,
                            "content": text.strip(),
                        })
                elif output_type == "execute_result":
                    text_data = data.get("text/plain", "")
                    if isinstance(text_data, list):
                        text_data = "".join(text_data)
                    if text_data.strip():
                        text_outputs.append({
                            "cell_index": cell_idx,
                            "content": text_data.strip(),
                        })

                # Image outputs (extractable as static files)
                if output_type in ("display_data", "execute_result"):
                    has_image = False
                    for mime in ("image/png", "image/jpeg", "image/svg+xml"):
                        if mime in data:
                            figures.append({
                                "cell_index": cell_idx,
                                "mime": mime,
                                "alt_text": "",
                            })
                            has_image = True

                    # HTML widget outputs (holoviews, bokeh, plotly, etc.)
                    # These render as interactive widgets in Jupyter but
                    # cannot be extracted as static images by nbconvert.
                    if not has_image and "text/html" in data:
                        html_content = data["text/html"]
                        if isinstance(html_content, list):
                            html_content = "".join(html_content)
                        # Detect interactive widget libraries
                        widget_lib = "unknown"
                        for lib, marker in (
                            ("holoviews", "HoloViews"),
                            ("bokeh", "Bokeh"),
                            ("plotly", "plotly"),
                            ("ipywidgets", "jupyter-widgets"),
                        ):
                            if marker in html_content:
                                widget_lib = lib
                                break
                        html_outputs.append({
                            "cell_index": cell_idx,
                            "widget_lib": widget_lib,
                            "html_length": len(html_content),
                        })

    # Save extracted figures to docs/reports/{name}/
    reports_dir = flow_dir / "docs" / "reports" / name
    extracted_outputs = resources.get("outputs", {})
    figures_extracted = 0
    figures_skipped = 0

    if extracted_outputs:
        if reports_dir.exists() and not overwrite:
            figures_skipped = len(extracted_outputs)
        else:
            reports_dir.mkdir(parents=True, exist_ok=True)
            for fname, fdata in extracted_outputs.items():
                (reports_dir / fname).write_bytes(fdata)
                figures_extracted += 1

    # Update figure paths to be relative to docs/reports/
    if figures_extracted > 0 or (reports_dir.exists() and figures_skipped > 0):
        fig_files = sorted(reports_dir.glob("*")) if reports_dir.exists() else []
        for i, fig in enumerate(figures):
            if i < len(fig_files):
                fig["path"] = f"{name}/{fig_files[i].name}"

    # Execution timestamp from notebook metadata
    executed_at = ""
    metadata = nb_data.get("metadata", {})
    if "papermill" in metadata:
        executed_at = metadata["papermill"].get("end_time", "")

    try:
        nb_rel = str(ipynb_path.relative_to(root))
    except ValueError:
        nb_rel = str(ipynb_path)

    try:
        reports_rel = str(reports_dir.relative_to(root))
    except ValueError:
        reports_rel = str(reports_dir)

    result: dict[str, Any] = {
        "flow": flow_name,
        "notebook": name,
        "format": "percent",
        "notebook_path": nb_rel,
        "figures_dir": reports_rel,
        "figures_extracted": figures_extracted,
        "figures_skipped": figures_skipped,
        "markdown_cells": markdown_cells,
        "figures": figures,
        "text_outputs": text_outputs,
        "execution_metadata": {
            "kernel": kernel,
            "executed_at": executed_at,
            "cell_count": len(nb_data.get("cells", [])),
            "tagged_cells": (
                len(markdown_cells) + len(figures) + len(text_outputs)
                if tags_only else None
            ),
        },
    }

    if html_outputs:
        result["html_outputs"] = html_outputs
        libs = sorted({h["widget_lib"] for h in html_outputs})
        result["html_outputs_hint"] = (
            f"{len(html_outputs)} interactive widget output(s) detected "
            f"({', '.join(libs)}). These render as HTML in Jupyter but cannot "
            "be extracted as static images. To include them in the report, "
            "add matplotlib summary plots alongside the interactive widgets, "
            "or use hv.save()/fig.write_image() to export static PNGs."
        )

    return result


def _nb_report_marimo(
    root: Path,
    flow_name: str,
    flow_dir: Path,
    py_path: Path,
    name: str,
    *,
    overwrite: bool,
    tags_only: bool,
) -> dict[str, Any]:
    """Extract report payload from a marimo-format notebook.

    Uses ``marimo export md`` to produce markdown with embedded figures,
    then parses the result.
    """
    import tempfile

    from pipeio.notebook.backend import resolve_backend

    backend = resolve_backend("marimo", py_path)

    # Export to markdown via marimo
    reports_dir = flow_dir / "docs" / "reports" / name

    with tempfile.TemporaryDirectory() as tmpdir:
        md_out = Path(tmpdir) / f"{name}.md"
        result = backend.export(
            py_path, output_format="md", output_path=md_out,
        )
        if not result.get("exported"):
            return {
                "error": (
                    f"Marimo export failed: {result.get('stderr', 'unknown error')}. "
                    "Ensure the notebook runs without errors."
                ),
            }

        md_text = md_out.read_text(encoding="utf-8") if md_out.exists() else ""

        # Extract embedded images (base64 data URIs in markdown)
        import re

        img_pattern = re.compile(
            r"!\[([^\]]*)\]\(data:image/(png|jpeg|svg\+xml);base64,([A-Za-z0-9+/=\s]+)\)"
        )
        figures: list[dict[str, Any]] = []
        figures_extracted = 0
        figures_skipped = 0

        if reports_dir.exists() and not overwrite:
            figures_skipped = len(img_pattern.findall(md_text))
        else:
            import base64

            reports_dir.mkdir(parents=True, exist_ok=True)
            for i, match in enumerate(img_pattern.finditer(md_text)):
                alt_text, mime_sub, b64data = match.group(1), match.group(2), match.group(3)
                ext = {"png": ".png", "jpeg": ".jpg", "svg+xml": ".svg"}.get(
                    mime_sub, ".png"
                )
                fname = f"output_{i}{ext}"
                (reports_dir / fname).write_bytes(base64.b64decode(b64data))
                figures.append({
                    "cell_index": i,
                    "path": f"{name}/{fname}",
                    "mime": f"image/{mime_sub}",
                    "alt_text": alt_text,
                })
                figures_extracted += 1

    # Split markdown into narrative sections (strip images already extracted)
    markdown_cells: list[dict[str, Any]] = []
    if md_text:
        # Remove base64 image blocks, keep text
        clean_md = img_pattern.sub("", md_text).strip()
        if clean_md:
            markdown_cells.append({"cell_index": 0, "content": clean_md})

    try:
        nb_rel = str(py_path.relative_to(root))
    except ValueError:
        nb_rel = str(py_path)

    try:
        reports_rel = str(reports_dir.relative_to(root))
    except ValueError:
        reports_rel = str(reports_dir)

    return {
        "flow": flow_name,
        "notebook": name,
        "format": "marimo",
        "notebook_path": nb_rel,
        "figures_dir": reports_rel,
        "figures_extracted": figures_extracted,
        "figures_skipped": figures_skipped,
        "markdown_cells": markdown_cells,
        "figures": figures,
        "text_outputs": [],
        "execution_metadata": {
            "kernel": "",
            "executed_at": "",
            "cell_count": 0,
            "tagged_cells": None,
        },
    }


def mcp_nb_report(
    root: Path,
    flow: str,
    name: str,
    *,
    overwrite: bool = False,
    tags_only: bool = False,
) -> dict[str, Any]:
    """Extract figures, markdown, and text outputs from an executed notebook.

    Saves extracted figures to ``{flow}/docs/reports/{name}/`` and returns
    a structured payload for the agent to write a curated report from.

    Supports both percent-format (ipynb) and marimo notebooks via the
    backend system. For percent-format, uses nbconvert extraction. For
    marimo, uses ``marimo export md``.

    Interactive widget outputs (holoviews, bokeh, plotly) that cannot be
    extracted as static images are reported in ``html_outputs`` with a
    hint on how to produce static alternatives.

    The report ``.md`` is **not** created by this tool — that is the agent's
    responsibility (via the ``/report`` skill or manual writing).

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
        overwrite: Re-extract figures even if the directory exists.
        tags_only: Only extract cells tagged with ``# REPORT:`` marker
            (percent-format only; ignored for marimo).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return {"error": f"Notebook not found: {name}"}

    # Resolve format via notebook.yml config or auto-detection
    fmt = ""
    kernel = ""
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            from pipeio.notebook.config import NotebookConfig
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for nb_entry in nb_cfg.entries:
                if Path(nb_entry.path).stem == name:
                    fmt = nb_cfg.resolve_format(nb_entry)
                    kernel = nb_cfg.resolve_kernel(nb_entry)
                    break
        except Exception:
            pass

    if not fmt:
        from pipeio.notebook.backend import detect_format
        fmt = detect_format(py_path)

    if fmt == "marimo":
        return _nb_report_marimo(
            root, entry.name, flow_dir, py_path, name,
            overwrite=overwrite, tags_only=tags_only,
        )

    return _nb_report_percent(
        root, entry.name, flow_dir, py_path, name,
        overwrite=overwrite, tags_only=tags_only, kernel=kernel,
    )


# ---------------------------------------------------------------------------
# MCP tools: notebook execution
# ---------------------------------------------------------------------------


def _python_prefix(python_bin: "str | list[str] | None") -> list[str]:
    """Normalise *python_bin* into a list suitable for subprocess cmd prefix."""
    if python_bin is None:
        return []
    if isinstance(python_bin, list):
        return list(python_bin)
    return [python_bin]


def _has_papermill(python: "str | list[str]") -> bool:
    """Check whether *python* can import papermill."""
    import subprocess

    prefix = _python_prefix(python)
    try:
        result = subprocess.run(
            [*prefix, "-c", "import papermill"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def mcp_nb_exec(
    root: Path,
    flow: str,
    name: str,
    params: dict[str, Any] | None = None,
    timeout: int = 600,
    python_bin: "str | list[str] | None" = None,
) -> dict[str, Any]:
    """Execute a notebook via papermill with optional parameter overrides.

    Syncs the notebook first (py → ipynb), then executes via papermill.
    Both jupytext and papermill are invoked from ``sys.executable`` (the MCP
    server's own Python).  The ``-k`` kernel flag controls which Jupyter
    kernel actually executes the notebook cells.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook basename (without extension).
        params: RunCard parameter overrides (injected into papermill).
        timeout: Cell execution timeout in seconds (default 600).
        python_bin: Ignored (kept for API compatibility). Jupytext and
            papermill always run from ``sys.executable``; cell execution
            is delegated to the Jupyter kernel specified in notebook.yml.
    """
    import subprocess
    import sys
    import time

    # Always use the MCP server's own Python for tooling (jupytext, papermill).
    # Cell execution is handled by the Jupyter kernel (-k flag), not python_bin.
    server_python: str = sys.executable

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return {"error": f"Notebook not found: {name}"}

    from pipeio.notebook.lifecycle import _nb_output_paths
    ipynb_path, _ = _nb_output_paths(py_path)

    # Resolve kernel from notebook.yml
    kernel = ""
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            from pipeio.notebook.config import NotebookConfig
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for nb in nb_cfg.entries:
                if Path(nb.path).stem == name:
                    kernel = nb_cfg.resolve_kernel(nb)
                    break
        except Exception:
            pass

    # Sync py → ipynb first (with kernel if configured)
    try:
        from pipeio.notebook.lifecycle import _require_jupytext, _jupytext
        _require_jupytext(python_bin=server_python)
        kernel_args: tuple[str, ...] = ("--set-kernel", kernel) if kernel else ()
        _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path),
                  *kernel_args, python_bin=server_python)
    except (ImportError, Exception) as exc:
        return {"error": f"Sync failed: {exc}"}

    # Build papermill command — same server_python, kernel handles cell execution
    if not _has_papermill(server_python):
        return {
            "error": (
                f"papermill not found in {server_python}. "
                "Install with: pip install papermill"
            ),
        }

    output_path = ipynb_path  # overwrite workspace .ipynb in-place
    cmd = [server_python, "-m", "papermill", str(ipynb_path), str(output_path),
           "--cwd", str(flow_dir)]
    if kernel:
        cmd.extend(["-k", kernel])

    if params:
        import json
        for key, value in params.items():
            cmd.extend(["-p", key, json.dumps(value) if not isinstance(value, str) else value])

    cmd.extend(["--execution-timeout", str(timeout)])

    start = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 30,
            cwd=str(root),
        )
        elapsed = round(time.monotonic() - start, 2)
    except subprocess.TimeoutExpired:
        elapsed = round(time.monotonic() - start, 2)
        return {
            "status": "timeout",
            "elapsed_seconds": elapsed,
            "timeout": timeout,
            "flow": entry.name,
            "name": name,
        }
    except FileNotFoundError:
        return {"error": f"Python binary not found: {server_python}"}

    try:
        output_rel = str(output_path.relative_to(root))
    except ValueError:
        output_rel = str(output_path)

    if result.returncode != 0:
        return {
            "status": "error",
            "elapsed_seconds": elapsed,
            "output_path": output_rel,
            "stderr": result.stderr[-2000:] if result.stderr else "",
            "flow": entry.name,
            "name": name,
        }

    return {
        "status": "ok",
        "elapsed_seconds": elapsed,
        "output_path": output_rel,
        "flow": entry.name,
        "name": name,
        "params_injected": list(params.keys()) if params else [],
    }


# ---------------------------------------------------------------------------
# MCP tools: notebook validation and watch
# ---------------------------------------------------------------------------


def _resolve_nb_with_config(
    root: Path, flow: str, name: str,
) -> tuple[Path | None, Any, Any]:
    """Resolve notebook path and its config entry for a flow.

    Returns ``(py_path, NotebookEntry or None, NotebookConfig or None)``.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return None, None, None

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        flow_entry = registry.get(flow)
    except (KeyError, ValueError):
        return None, None, None

    flow_dir = Path(flow_entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return None, None, None

    # Load config for format resolution
    nb_cfg = None
    nb_entry = None
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            from pipeio.notebook.config import NotebookConfig
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
            for e in nb_cfg.entries:
                if Path(e.path).stem == name:
                    nb_entry = e
                    break
        except Exception:
            pass

    return py_path, nb_entry, nb_cfg


def mcp_nb_validate(
    root: Path,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Validate notebook structure.

    For percent-format: syntax-checks each cell, validates import isolation,
    checks for variable shadowing.
    For marimo: runs ``marimo check`` for DAG validation (cycle detection,
    missing dependencies, undefined names).

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook stem name.

    Returns:
        dict with ``valid`` (bool), ``issues`` or ``stdout``/``stderr``,
        ``format``.
    """
    from pipeio.notebook.backend import resolve_backend

    py_path, nb_entry, nb_cfg = _resolve_nb_with_config(root, flow, name)
    if py_path is None:
        return {"error": f"Notebook '{name}' not found in flow '{flow}'"}

    fmt = ""
    if nb_cfg is not None and nb_entry is not None:
        fmt = nb_cfg.resolve_format(nb_entry)
    backend = resolve_backend(fmt, py_path)
    return backend.validate(py_path)


def mcp_nb_watch(
    root: Path,
    flow: str,
    name: str,
    port: int = 0,
) -> dict[str, Any]:
    """Launch ``marimo edit --watch`` for live human oversight.

    Only supported for marimo-format notebooks. Returns the URL
    for the browser UI and the PID for later termination.

    For percent-format notebooks, returns an error suggesting ``nb_lab``
    instead.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook stem name.
        port: Optional port for the marimo server (0 = auto).
    """
    import subprocess as sp

    from pipeio.notebook.backend import resolve_backend

    py_path, nb_entry, nb_cfg = _resolve_nb_with_config(root, flow, name)
    if py_path is None:
        return {"error": f"Notebook '{name}' not found in flow '{flow}'"}

    fmt = ""
    if nb_cfg is not None and nb_entry is not None:
        fmt = nb_cfg.resolve_format(nb_entry)
    backend = resolve_backend(fmt, py_path)

    if backend.name != "marimo":
        return {
            "error": f"nb_watch is only supported for marimo notebooks (this is {backend.name}). "
            "For percent-format notebooks, use nb_lab to open in Jupyter Lab.",
        }

    cmd = [*backend._marimo_cmd, "edit", str(py_path), "--watch"]
    if port:
        cmd.extend(["--port", str(port)])

    try:
        proc = sp.Popen(
            cmd,
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            cwd=str(root),
        )
        return {
            "status": "started",
            "pid": proc.pid,
            "command": " ".join(cmd),
            "notebook": str(py_path),
            "flow": flow,
            "name": name,
            "hint": "Open the URL shown in terminal output. Use kill(pid) to stop.",
        }
    except FileNotFoundError:
        return {"error": "marimo command not found. Install with: pip install marimo"}


def mcp_nb_snapshot(
    root: Path,
    flow: str,
    name: str,
    timeout: int = 120,
    max_text_length: int = 2000,
) -> dict[str, Any]:
    """Capture a marimo session snapshot: execute all cells and return outputs.

    Runs ``marimo export session`` to produce a JSON snapshot, then parses
    it into a structured summary with cell outputs, console (stdout/stderr),
    and errors.  Large binary MIME data (images) is replaced with a size
    placeholder to keep the response manageable.

    This is the agent's "eyes" — it sees what the human sees in the marimo
    browser UI.

    Args:
        root: Project root.
        flow: Flow name.
        name: Notebook stem name.
        timeout: Execution timeout in seconds (default 120).
        max_text_length: Truncate text outputs longer than this (default 2000).

    Returns:
        dict with ``cells`` (list of cell summaries), ``errors`` (bool),
        ``cell_count``, ``error_count``.
    """
    import json as _json
    import subprocess as sp
    import tempfile

    from pipeio.notebook.backend import resolve_backend

    py_path, nb_entry, nb_cfg = _resolve_nb_with_config(root, flow, name)
    if py_path is None:
        return {"error": f"Notebook '{name}' not found in flow '{flow}'"}

    fmt = ""
    if nb_cfg is not None and nb_entry is not None:
        fmt = nb_cfg.resolve_format(nb_entry)
    backend = resolve_backend(fmt, py_path)

    if backend.name != "marimo":
        return {
            "error": f"nb_snapshot is only supported for marimo notebooks (this is {backend.name}). "
            "For percent-format, use nb_exec + nb_read to inspect ipynb outputs.",
        }

    # Run marimo export session to a temp dir
    with tempfile.TemporaryDirectory() as tmpdir:
        session_dir = Path(tmpdir) / "__marimo__" / "session"
        cmd = [
            *backend._marimo_cmd, "export", "session",
            str(py_path),
            "--force-overwrite",
        ]

        try:
            result = sp.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(py_path.parent),
                env={**__import__("os").environ, "MARIMO_SESSION_DIR": str(session_dir)},
            )
        except sp.TimeoutExpired:
            return {"error": f"Snapshot timed out after {timeout}s"}
        except FileNotFoundError:
            return {"error": "marimo command not found"}

        # Find the session JSON — marimo writes to __marimo__/session/ next to the notebook
        # or in the working directory
        session_json = None
        for candidate_dir in [
            py_path.parent / "__marimo__" / "session",
            session_dir,
            Path(tmpdir),
        ]:
            if candidate_dir.exists():
                for f in candidate_dir.glob("*.json"):
                    session_json = f
                    break
            if session_json:
                break

        if session_json is None:
            # Try parsing stdout as JSON (some versions output to stdout)
            stderr_text = result.stderr[-1000:] if result.stderr else ""
            return {
                "error": f"Session snapshot not found after export. "
                f"returncode={result.returncode}",
                "stderr": stderr_text,
                "stdout": result.stdout[-500:] if result.stdout else "",
            }

        try:
            snapshot = _json.loads(session_json.read_text(encoding="utf-8"))
        except Exception as exc:
            return {"error": f"Failed to parse session JSON: {exc}"}

    # Parse snapshot into agent-friendly summary
    cells_summary: list[dict[str, Any]] = []
    error_count = 0

    for cell in snapshot.get("cells", []):
        cell_info: dict[str, Any] = {
            "id": cell.get("id", ""),
        }

        # Console output (prints, stderr)
        console_lines: list[str] = []
        for entry in cell.get("console", []):
            if entry.get("type") == "stream":
                text = entry.get("text", "")
                if len(text) > max_text_length:
                    text = text[:max_text_length] + f"\n... (truncated, {len(entry['text'])} chars total)"
                prefix = "" if entry.get("name") == "stdout" else "[stderr] "
                console_lines.append(f"{prefix}{text}")
        if console_lines:
            cell_info["console"] = "\n".join(console_lines)

        # Cell outputs
        for output in cell.get("outputs", []):
            if output.get("type") == "error":
                cell_info["error"] = {
                    "name": output.get("ename", ""),
                    "message": output.get("evalue", ""),
                    "traceback": output.get("traceback", [])[-5:],  # last 5 frames
                }
                error_count += 1
            elif output.get("type") == "data":
                data = output.get("data", {})
                # Prefer text/plain, then text/html (truncated), skip binary
                if "text/plain" in data:
                    text = data["text/plain"]
                    if len(text) > max_text_length:
                        text = text[:max_text_length] + f"\n... (truncated)"
                    cell_info["output_text"] = text
                elif "text/html" in data:
                    html = data["text/html"]
                    if len(html) > max_text_length:
                        html = html[:max_text_length] + "\n... (truncated)"
                    cell_info["output_html"] = html
                # Note binary outputs without including them
                for mime in data:
                    if mime.startswith("image/"):
                        cell_info["has_image"] = True
                        cell_info["image_mime"] = mime
                        break

        # Only include cells with actual output
        if len(cell_info) > 1:  # more than just "id"
            cells_summary.append(cell_info)

    return {
        "cells": cells_summary,
        "cell_count": len(snapshot.get("cells", [])),
        "output_cells": len(cells_summary),
        "error_count": error_count,
        "has_errors": error_count > 0,
        "notebook": str(py_path),
        "flow": flow,
        "name": name,
    }


# ---------------------------------------------------------------------------
# MCP tools: snakemake native DAG export and report
# ---------------------------------------------------------------------------


def mcp_dag_export(
    root: Path,
    flow: str | None = None,
    graph_type: str = "rulegraph",
    output_format: str = "dot",
    snakemake_cmd: list[str] | None = None,
) -> dict[str, Any]:
    """Export rule/job DAG via snakemake's native graph output.

    Shells out to ``snakemake --rulegraph`` (or ``--dag``, ``--d3dag``)
    and optionally converts dot output to SVG via graphviz.

    When ``output_format="svg"``, the SVG is automatically written to
    ``docs/pipelines/<pipe>/<flow>/dag.svg`` for site integration.
    Other formats (dot, mermaid, json) return content only.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        graph_type: ``rulegraph`` (rule-level, compact), ``dag`` (job-level),
            or ``d3dag`` (JSON for D3.js).
        output_format: ``dot``, ``mermaid``, ``svg`` (requires graphviz), or
            ``json`` (only with d3dag).
        snakemake_cmd: Command tokens to invoke snakemake (e.g.
            ``["conda", "run", "-n", "cogpy", "snakemake"]``).
    """
    import subprocess

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        return {"error": f"No Snakefile in {flow_dir.relative_to(root)}"}

    snake_base = snakemake_cmd or ["snakemake"]

    # Build the snakemake graph command
    # --dag and --rulegraph accept an optional format via nargs="?"
    # (choices: "dot" (default), "mermaid-js").  --d3dag is boolean.
    if graph_type == "d3dag":
        cmd = [*snake_base, "--snakefile", str(snakefile),
               "--directory", str(flow_dir), "--d3dag"]
    elif graph_type == "dag":
        fmt_arg = "mermaid-js" if output_format == "mermaid" else "dot"
        cmd = [*snake_base, "--snakefile", str(snakefile),
               "--directory", str(flow_dir), "--dag", fmt_arg]
    else:  # rulegraph (default)
        fmt_arg = "mermaid-js" if output_format == "mermaid" else "dot"
        cmd = [*snake_base, "--snakefile", str(snakefile),
               "--directory", str(flow_dir), "--rulegraph", fmt_arg]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(root),
            timeout=60, check=False,
        )
    except subprocess.TimeoutExpired:
        return {"error": "Snakemake graph generation timed out (60s)"}

    if result.returncode != 0:
        return {"error": f"Snakemake failed: {result.stderr[:1000]}"}

    graph_output = result.stdout

    # Optionally convert dot to SVG
    if output_format == "svg" and graph_type != "d3dag":
        dot_bin = _find_dot()
        if dot_bin is None:
            return {
                "error": "graphviz 'dot' not found — install with: conda install -c conda-forge graphviz",
                "dot": graph_output,
            }
        try:
            svg_result = subprocess.run(
                [dot_bin, "-Tsvg"], input=graph_output, capture_output=True,
                text=True, timeout=30, check=False,
            )
            if svg_result.returncode != 0:
                return {
                    "error": f"graphviz dot failed: {svg_result.stderr[:500]}",
                    "dot": graph_output,
                }
            graph_output = svg_result.stdout
        except FileNotFoundError:
            return {
                "error": f"graphviz 'dot' not found at {dot_bin}",
                "dot": graph_output,
            }

    actual_format = "json" if graph_type == "d3dag" else output_format

    # SVG → write to .build/ only — docs_collect handles the copy to docs/
    written_path = None
    if output_format == "svg":
        flow_dir = Path(entry.code_path)
        if not flow_dir.is_absolute():
            flow_dir = root / flow_dir
        build_out = flow_dir / ".build" / "dag.svg"
        build_out.parent.mkdir(parents=True, exist_ok=True)
        build_out.write_text(graph_output, encoding="utf-8")
        written_path = str(build_out.relative_to(root))
        # Inject DAG link into the source overview/index if not already present
        _inject_dag_link_in_source(flow_dir)

    result_dict: dict[str, Any] = {
        "flow": entry.name,
        "graph_type": graph_type,
        "format": actual_format,
    }
    if written_path:
        result_dict["written"] = written_path
    else:
        result_dict["output"] = graph_output
    return result_dict


def mcp_report(
    root: Path,
    flow: str | None = None,
    output_path: str = "",
    target: str = "",
    snakemake_cmd: list[str] | None = None,
) -> dict[str, Any]:
    """Generate a snakemake HTML report for a flow.

    Uses ``snakemake --report`` to produce a self-contained HTML report
    with runtime statistics, provenance, and annotated outputs.

    Automatically resolves existing output files from the flow's registry
    and passes them as explicit targets, so the report succeeds even when
    some outputs are missing (no ``rule report`` needed in the Snakefile).

    When ``target`` is specified (e.g. a rule name), it overrides the
    auto-resolution and uses that target directly.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        output_path: Where to write the report (relative to root).
            Defaults to ``derivatives/{flow}/report.html``.
        target: Explicit target rule (overrides auto-resolution).
        snakemake_cmd: Command tokens to invoke snakemake.
    """
    import subprocess

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        return {"error": f"No Snakefile in {flow_dir.relative_to(root)}"}

    snake_base = snakemake_cmd or ["snakemake"]

    # Determine output path
    if not output_path:
        output_path = f"derivatives/{entry.name}/report.html"
    report_abs = root / output_path
    report_abs.parent.mkdir(parents=True, exist_ok=True)

    # Auto-resolve existing outputs as targets if no explicit target given
    targets: list[str] = []
    if target:
        targets = [target]
    else:
        try:
            from pipeio.config import FlowConfig
            from pipeio.resolver import PipelineContext

            if entry.config_path:
                cfg_path = Path(entry.config_path)
                if not cfg_path.is_absolute():
                    cfg_path = root / cfg_path
                if cfg_path.exists():
                    flow_config = FlowConfig.from_yaml(cfg_path)
                    ctx = PipelineContext.from_config(flow_config, root)
                    for group_name in ctx.groups():
                        for member_name in ctx.products(group_name):
                            existing = ctx.expand(group_name, member_name)
                            targets.extend(str(p) for p in existing)
        except Exception:
            pass  # fall through to no-target mode

    if not targets:
        return {
            "error": "No existing outputs found to report on. "
            "Run the pipeline first, or specify a target rule.",
        }

    # Build command
    cmd = [
        *snake_base,
        "--snakefile", str(snakefile),
        "--directory", str(flow_dir),
        "--report", str(report_abs),
        *targets,
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(root),
            timeout=300, check=False,
        )
    except subprocess.TimeoutExpired:
        return {"error": "Snakemake report generation timed out (5min)"}

    if result.returncode != 0:
        return {"error": f"Snakemake report failed: {result.stderr[:1000]}"}

    return {
        "flow": entry.name,
        "report_path": output_path,
        "exists": report_abs.exists(),
        "size_kb": round(report_abs.stat().st_size / 1024, 1) if report_abs.exists() else 0,
        "targets_resolved": len(targets),
    }


# ---------------------------------------------------------------------------
# MCP tools: completion tracking
# ---------------------------------------------------------------------------


def mcp_completion(
    root: Path,
    flow: str | None = None,
    mod: str | None = None,
) -> dict[str, Any]:
    """Check session completion by comparing expected outputs against filesystem.

    For each registry group and member, expands the output directory to find
    existing files, then reports which sessions are complete, partial, or missing.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        mod: Filter to a specific mod's output groups (optional).
    """
    from pipeio.config import FlowConfig
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if not entry.config_path:
        return {"error": f"No config_path for {entry.name}"}

    cfg_path = Path(entry.config_path)
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path
    if not cfg_path.exists():
        return {"error": f"Config not found: {cfg_path}"}

    try:
        cfg = FlowConfig.from_yaml(cfg_path)
    except Exception as exc:
        return {"error": f"Failed to parse config: {exc}"}

    # Get output directory
    output_base = root / cfg.output_dir if cfg.output_dir else root

    # Collect groups to check
    groups_to_check = list(cfg.registry.keys())
    if mod and entry.mods.get(mod):
        # Filter to groups whose names match mod rules' output patterns
        mod_rules = set(entry.mods[mod].rules)
        # We keep all groups since we can't filter without running Snakemake
        # but mention the mod context
        pass

    group_results: list[dict[str, Any]] = []

    for group_name, group in cfg.registry.items():
        group_root = group.bids.get("root", group_name)
        group_dir = output_base / group_root

        member_names = list(group.members.keys())
        member_results: list[dict[str, Any]] = []

        for member_name, member in group.members.items():
            # Glob for matching files
            pattern = f"**/*{member.suffix}{member.extension}"
            if group_dir.exists():
                matches = sorted(group_dir.glob(pattern))
            else:
                matches = []

            member_results.append({
                "member": member_name,
                "suffix": member.suffix,
                "extension": member.extension,
                "file_count": len(matches),
                "files": [str(p.relative_to(root)) for p in matches[:20]],
                "truncated": len(matches) > 20,
            })

        # Derive session IDs from file paths (extract entity patterns)
        all_files: list[Path] = []
        for mr in member_results:
            for f in mr.get("files", []):
                all_files.append(root / f)

        # Extract unique session entity strings from paths
        session_entities: set[str] = set()
        entity_re = re.compile(r"((?:sub|ses|run|task|acq|rec|dir|space|desc)-[^_/]+)")
        for fp in all_files:
            entities = sorted(entity_re.findall(str(fp)))
            if entities:
                session_entities.add("_".join(entities))

        # Determine completeness per session
        total_members = len(member_names)
        complete_sessions = 0
        partial_sessions = 0
        for sess_key in session_entities:
            members_found = 0
            for mr in member_results:
                if any(sess_key.replace("_", "") in f.replace("_", "") or
                       all(e in f for e in sess_key.split("_"))
                       for f in mr.get("files", [])):
                    members_found += 1
            if members_found == total_members:
                complete_sessions += 1
            elif members_found > 0:
                partial_sessions += 1

        group_results.append({
            "group": group_name,
            "group_dir": str(group_dir.relative_to(root)) if group_dir.exists() else None,
            "dir_exists": group_dir.exists(),
            "members": member_results,
            "sessions_found": len(session_entities),
            "complete": complete_sessions,
            "partial": partial_sessions,
            "missing": len(session_entities) - complete_sessions - partial_sessions,
        })

    return {
        "flow": entry.name,
        "flow": entry.name,
        "output_dir": cfg.output_dir,
        "groups": group_results,
    }


# ---------------------------------------------------------------------------
# MCP tools: target path resolution
# ---------------------------------------------------------------------------


def mcp_target_paths(
    root: Path,
    flow: str | None = None,
    group: str = "",
    member: str = "",
    entities: dict[str, str] | None = None,
    expand: bool = False,
) -> dict[str, Any]:
    """Resolve output paths for a flow's registry entries.

    Uses the ``PipelineContext`` / ``PathResolver`` to translate
    (group, member, entities) tuples into concrete filesystem paths —
    the same paths snakemake would target.

    **Modes:**

    - *resolve* (default): given group + member + entities, returns the
      single resolved path and whether it exists on disk.
    - *expand* (``expand=True``): glob the filesystem for all matching
      paths, optionally filtered by entities (e.g. ``sub=01``).
    - *list groups* (no group/member): returns available groups and
      members from the output registry so you know what to ask for.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        group: Registry group name (e.g. ``preproc``, ``spectral``).
        member: Registry member name (e.g. ``cleaned``, ``psd``).
        entities: Wildcard entities for path resolution
            (e.g. ``{"sub": "01", "ses": "04"}``).
        expand: If True, enumerate all matching paths on disk instead
            of resolving a single path.
    """
    from pipeio.resolver import PipelineContext

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    from pipeio.registry import PipelineRegistry

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    try:
        ctx = PipelineContext.from_registry(
            flow, root=root, registry=registry,
        )
    except (FileNotFoundError, Exception) as exc:
        return {"error": str(exc)}

    # -- List mode: no group specified → show available groups/members --
    if not group:
        groups_info = {}
        for g in ctx.groups():
            members = ctx.products(g)
            pattern = {m: ctx.pattern(g, m) for m in members}
            groups_info[g] = {"members": members, "patterns": pattern}
        return {
            "flow": entry.name,
            "flow": entry.name,
            "mode": "list",
            "groups": groups_info,
        }

    # -- Expand mode: glob for all matching paths on disk --
    if expand:
        ent = entities or {}
        if member:
            # Expand a single member
            paths = ctx.expand(group, member, **ent)
            return {
                "flow": entry.name,
                "flow": entry.name,
                "mode": "expand",
                "group": group,
                "member": member,
                "entities_filter": ent,
                "paths": [str(p) for p in paths],
                "count": len(paths),
            }
        else:
            # Expand all members in the group
            all_results: dict[str, Any] = {}
            for m in ctx.products(group):
                paths = ctx.expand(group, m, **ent)
                all_results[m] = {
                    "paths": [str(p) for p in paths],
                    "count": len(paths),
                }
            return {
                "flow": entry.name,
                "flow": entry.name,
                "mode": "expand",
                "group": group,
                "entities_filter": ent,
                "members": all_results,
            }

    # -- Resolve mode: single path from group + member + entities --
    if not member:
        return {"error": "member is required for resolve mode (or use expand=True)"}

    ent = entities or {}
    if not ent:
        # No entities: show the pattern template instead
        pattern = ctx.pattern(group, member)
        return {
            "flow": entry.name,
            "flow": entry.name,
            "mode": "pattern",
            "group": group,
            "member": member,
            "pattern": pattern,
        }

    try:
        path = ctx.path(group, member, **ent)
    except KeyError as exc:
        return {"error": str(exc)}

    return {
        "flow": entry.name,
        "flow": entry.name,
        "mode": "resolve",
        "group": group,
        "member": member,
        "entities": ent,
        "path": str(path),
        "exists": path.exists(),
    }


# ---------------------------------------------------------------------------
# MCP tools: cross-flow registry chain tracking
# ---------------------------------------------------------------------------


def mcp_cross_flow(
    root: Path,
    flow: str | None = None,
) -> dict[str, Any]:
    """Map output_manifest → input_manifest chains across flows.

    For each flow that declares an ``input_manifest`` (or legacy
    ``input_registry``) in its config, finds which other flow's
    ``output_manifest`` matches.  Detects stale or broken references.

    Args:
        root: Project root.
        flow: Filter by flow name (optional).
    """
    import yaml
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    all_flows = registry.list_flows()

    # Build flow metadata with input/output manifest paths
    flow_meta: list[dict[str, Any]] = []
    for entry in all_flows:
        if not entry.config_path:
            continue
        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        if not cfg_path.exists():
            continue

        try:
            raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue

        input_manifest = raw.get("input_manifest", "")
        output_manifest = raw.get("output_manifest", "")

        flow_meta.append({
            "flow": entry.name,
            "input_dir": raw.get("input_dir", ""),
            "input_manifest": input_manifest,
            "output_dir": raw.get("output_dir", ""),
            "output_manifest": output_manifest,
        })

    # Build output_manifest → flow mapping
    output_map: dict[str, str] = {}
    for fm in flow_meta:
        if fm["output_manifest"]:
            output_map[fm["output_manifest"]] = fm["flow"]

    # Build chains: for each flow with input_manifest, find its source
    chains: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []

    for fm in flow_meta:
        if not fm["input_manifest"]:
            continue

        input_manifest = fm["input_manifest"]
        source_flow = output_map.get(input_manifest)

        chain_entry: dict[str, Any] = {
            "consumer": fm["flow"],
            "input_manifest": input_manifest,
            "producer": source_flow,
        }

        if source_flow:
            dir_exists = (root / input_manifest).exists() if input_manifest else False
            chain_entry["dir_exists"] = dir_exists
            chains.append(chain_entry)
        else:
            chain_entry["status"] = "unresolved"
            chain_entry["hint"] = "No flow outputs to this manifest path"
            stale.append(chain_entry)

    # Filter to specific flow if requested
    if flow:
        chains = [c for c in chains if flow in (c.get("consumer", ""), c.get("producer", ""))]
        stale = [s for s in stale if flow in s.get("consumer", "")]

    return {
        "chains": chains,
        "stale": stale,
        "flows_analyzed": len(flow_meta),
    }


# ---------------------------------------------------------------------------
# MCP tools: Snakemake log parsing
# ---------------------------------------------------------------------------


def mcp_log_parse(
    root: Path,
    flow: str | None = None,
    run_id: str | None = None,
    log_path: str | None = None,
) -> dict[str, Any]:
    """Extract structured data from Snakemake log files.

    Parses logs to find completed rules with timing, failed rules with error
    summaries, resource warnings, and missing inputs.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        run_id: Specific run ID from .pipeio/runs.json (optional).
        log_path: Direct path to a Snakemake log file (optional).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    # Resolve log file
    target_log: Path | None = None
    if log_path:
        target_log = Path(log_path) if Path(log_path).is_absolute() else root / log_path
    elif run_id:
        runs_file = root / ".pipeio" / "runs.json"
        if not runs_file.exists():
            runs_file = root / ".projio" / "pipeio" / "runs.json"
        if runs_file.exists():
            import json
            try:
                runs = json.loads(runs_file.read_text(encoding="utf-8"))
                for run in runs:
                    if run.get("id") == run_id:
                        target_log = Path(run["log_path"])
                        break
            except Exception:
                pass
        if target_log is None:
            return {"error": f"Run ID {run_id!r} not found in runs.json"}
    else:
        # Find most recent .snakemake/log
        log_dir = flow_dir / ".snakemake" / "log"
        if not log_dir.exists():
            # Try project-root level
            log_dir = root / ".snakemake" / "log"
        if log_dir.exists():
            logs = sorted(log_dir.glob("*.snakemake.log"), reverse=True)
            if logs:
                target_log = logs[0]

    if target_log is None or not target_log.exists():
        return {"error": "No Snakemake log found", "hint": "Provide log_path or run_id"}

    try:
        log_text = target_log.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return {"error": f"Failed to read log: {exc}"}

    # Parse log content
    completed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    warnings: list[str] = []
    missing_inputs: list[str] = []

    # Patterns for Snakemake log entries
    rule_start_re = re.compile(
        r"^\[.+?\]\s+rule\s+(\w+):\s*$", re.MULTILINE
    )
    finished_re = re.compile(
        r"^\[.+?\]\s+Finished job (\d+)\.\s*$", re.MULTILINE
    )
    # "localrule X:" or "rule X:" with timing
    timing_re = re.compile(
        r"^\[.+?\]\s+(?:local)?rule\s+(\w+):\s*$", re.MULTILINE
    )
    error_re = re.compile(
        r"^Error in rule (\w+):", re.MULTILINE
    )
    error_block_re = re.compile(
        r"Error in rule (\w+):\s*\n((?:\s+.+\n)*)", re.MULTILINE
    )
    missing_re = re.compile(
        r"^MissingInputException.+?:\n((?:\s+.+\n)*)", re.MULTILINE
    )
    resource_re = re.compile(
        r"^(Warning:.+resource.+)$", re.MULTILINE | re.IGNORECASE
    )

    # Extract completed rules with timing info
    complete_re = re.compile(
        r"\[.+?\]\s+Finished job \d+\.\s*\n\s*\d+ of \d+ steps \(\d+%\) done",
        re.MULTILINE,
    )
    # Simpler: track rule mentions and job completions
    rule_done_re = re.compile(
        r"^\[(.+?)\]\s+Finished job \d+\.", re.MULTILINE
    )
    rule_exec_re = re.compile(
        r"^\[(.+?)\]\s+(?:local)?rule\s+(\w+):", re.MULTILINE
    )

    # Build timeline: (timestamp, event, rule_name)
    for m in rule_exec_re.finditer(log_text):
        completed.append({
            "rule": m.group(2),
            "timestamp": m.group(1),
            "status": "started",
        })

    # Extract errors
    for m in error_block_re.finditer(log_text):
        rule_name = m.group(1)
        detail = m.group(2).strip()[:500]
        failed.append({"rule": rule_name, "error": detail})

    # Extract missing inputs
    for m in missing_re.finditer(log_text):
        missing_inputs.extend(
            line.strip() for line in m.group(1).splitlines() if line.strip()
        )

    # Extract resource warnings
    for m in resource_re.finditer(log_text):
        warnings.append(m.group(1).strip())

    # Count summary line
    summary_re = re.compile(r"(\d+) of (\d+) steps \((\d+)%\) done")
    summary_match = summary_re.search(log_text)
    summary: dict[str, Any] | None = None
    if summary_match:
        summary = {
            "completed": int(summary_match.group(1)),
            "total": int(summary_match.group(2)),
            "percent": int(summary_match.group(3)),
        }

    try:
        log_rel = str(target_log.relative_to(root))
    except ValueError:
        log_rel = str(target_log)

    return {
        "flow": entry.name,
        "flow": entry.name,
        "log_path": log_rel,
        "rules_started": len(completed),
        "rules_failed": len(failed),
        "completed_rules": completed,
        "failed_rules": failed,
        "missing_inputs": missing_inputs[:20],
        "resource_warnings": warnings,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# MCP tools: managed Snakemake execution
# ---------------------------------------------------------------------------


def _runs_path(root: Path) -> Path:
    """Return path to the runs state file."""
    for candidate in (
        root / ".projio" / "pipeio" / "runs.json",
        root / ".pipeio" / "runs.json",
    ):
        if candidate.parent.exists():
            return candidate
    out = root / ".pipeio"
    out.mkdir(exist_ok=True)
    return out / "runs.json"


def _load_runs(root: Path) -> list[dict[str, Any]]:
    """Load runs state from JSON file."""
    import json
    rp = _runs_path(root)
    if rp.exists():
        try:
            return json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def _save_runs(root: Path, runs: list[dict[str, Any]]) -> None:
    """Save runs state to JSON file."""
    import json
    rp = _runs_path(root)
    rp.write_text(json.dumps(runs, indent=2), encoding="utf-8")


def mcp_run(
    root: Path,
    flow: str | None = None,
    targets: list[str] | None = None,
    cores: int = 1,
    dryrun: bool = False,
    keep_going: bool = True,
    forcerun: list[str] | None = None,
    forceall: bool = False,
    touch: bool = False,
    retries: int = 0,
    extra_args: list[str] | None = None,
    snakemake_cmd: list[str] | None = None,
    wildcards: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Launch Snakemake in a detached screen session.

    State is tracked in ``.pipeio/runs.json``.

    Args:
        root: Project root.
        flow: Flow name (optional for single-flow pipes).
        targets: Snakemake target rules (optional).
        cores: Number of cores (default 1).
        dryrun: If True, pass ``-n`` for a dry run.
        keep_going: Continue with independent jobs after a failure
            (default True).
        forcerun: Force re-execution of specific rules (e.g.
            ``["badlabel", "interpolate"]``).
        forceall: Force execution of all rules regardless of timestamps.
        touch: Mark outputs as up-to-date without executing (``-t``).
        retries: Number of times to retry failing jobs (default 0).
        extra_args: Additional Snakemake CLI arguments.
        snakemake_cmd: Command tokens to invoke snakemake (e.g.
            ``["conda", "run", "-n", "cogpy", "snakemake"]``).
            Defaults to ``["snakemake"]``.
        wildcards: Entity filters for scoping runs (e.g.
            ``{"subject": "01", "session": "04"}``).  Maps to
            snakebids ``--filter-{key} {value}`` CLI flags.
    """
    import json
    import shutil
    import subprocess
    import time
    import uuid

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if shutil.which("screen") is None:
        return {"error": "screen is not installed. Install with: apt install screen"}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        return {"error": f"No Snakefile in {flow_dir.relative_to(root)}"}

    # Auto-unlock stale locks: if lock files exist but no active screen
    # session is running for this flow, unlock before proceeding.
    lock_dir = flow_dir / ".snakemake" / "locks"
    if lock_dir.is_dir() and any(lock_dir.iterdir()):
        # Check if any pipeio screen for this flow is still alive
        try:
            screen_out = subprocess.run(
                ["screen", "-ls"], capture_output=True, text=True,
            )
            flow_screen_alive = f"pipeio-{entry.name}-" in screen_out.stdout
        except Exception:
            flow_screen_alive = False

        if not flow_screen_alive:
            snake_unlock = snakemake_cmd or ["snakemake"]
            unlock_cmd = [
                *snake_unlock,
                "--snakefile", str(snakefile),
                "--directory", str(flow_dir),
                "--unlock",
            ]
            subprocess.run(
                unlock_cmd, capture_output=True, cwd=str(root), timeout=30,
            )

    # Generate run ID and log path
    run_id = str(uuid.uuid4())[:8]
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_dir = flow_dir / ".snakemake" / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run-{timestamp}-{run_id}.log"

    # Build Snakemake command
    snake_base = snakemake_cmd or ["snakemake"]
    snake_cmd = [
        *snake_base,
        "--snakefile", str(snakefile),
        "--directory", str(flow_dir),
        "--cores", str(cores),
        # Always-on robustness flags
        "--rerun-incomplete",
        "--latency-wait", "15",
        "--printshellcmds",
        "--show-failed-logs",
    ]
    if keep_going:
        snake_cmd.append("--keep-going")
    if dryrun:
        snake_cmd.append("-n")
    if touch:
        snake_cmd.append("--touch")
    if forceall:
        snake_cmd.append("--forceall")
    elif forcerun:
        snake_cmd.extend(["--forcerun", *forcerun])
    if retries > 0:
        snake_cmd.extend(["--retries", str(retries)])
    if targets:
        snake_cmd.extend(targets)
    if wildcards:
        # snakebids --filter-{entity} {value} for scoping
        for key, value in wildcards.items():
            snake_cmd.extend([f"--filter-{key}", str(value)])
    if extra_args:
        snake_cmd.extend(extra_args)

    screen_name = f"pipeio-{entry.name}-{run_id}"
    # Use stdbuf to force line-buffered stdout so log is visible during
    # long-running steps (e.g. BIDS indexing under conda run).
    stdbuf_prefix = "stdbuf -oL " if shutil.which("stdbuf") else ""
    # Run via screen, logging output
    full_cmd = [
        "screen", "-dmS", screen_name,
        "bash", "-c",
        f"{stdbuf_prefix}{' '.join(snake_cmd)} 2>&1 | tee {log_path}; echo EXIT_CODE=$? >> {log_path}",
    ]

    try:
        subprocess.run(full_cmd, check=True, capture_output=True, cwd=str(root))
    except subprocess.CalledProcessError as exc:
        return {"error": f"Failed to launch screen: {exc.stderr.decode()[:500]}"}

    # Record run
    runs = _load_runs(root)
    run_record = {
        "id": run_id,
        "flow": entry.name,
        "screen": screen_name,
        "log_path": str(log_path.relative_to(root)),
        "started_at": timestamp,
        "status": "running",
        "dryrun": dryrun,
        "cores": cores,
    }
    runs.append(run_record)
    _save_runs(root, runs)

    return {
        "launched": True,
        "run_id": run_id,
        "screen": screen_name,
        "log_path": str(log_path.relative_to(root)),
        "flow": entry.name,
        "dryrun": dryrun,
    }


def mcp_run_status(
    root: Path,
    run_id: str | None = None,
    flow: str | None = None,
) -> dict[str, Any]:
    """Query progress of running or recent Snakemake runs.

    Checks screen sessions and parses the tail of log files for progress.

    Args:
        root: Project root.
        run_id: Specific run ID to query (optional).
        flow: Filter by flow (optional).
    """
    import subprocess

    runs = _load_runs(root)
    if not runs:
        return {"runs": [], "message": "No runs recorded"}

    # Filter
    if run_id:
        runs = [r for r in runs if r["id"] == run_id]
    if flow:
        runs = [r for r in runs if r.get("flow") == flow]

    # Check screen sessions
    try:
        screen_out = subprocess.run(
            ["screen", "-ls"], capture_output=True, text=True,
        )
        active_screens = screen_out.stdout
    except Exception:
        active_screens = ""

    results: list[dict[str, Any]] = []
    for run in runs:
        info = dict(run)

        # Check if screen is still alive
        screen_name = run.get("screen", "")
        info["screen_alive"] = screen_name in active_screens

        # Update status
        if not info["screen_alive"] and run.get("status") == "running":
            info["status"] = "finished"

        # Parse log tail for progress
        log_path = root / run.get("log_path", "")
        if log_path.exists():
            try:
                text = log_path.read_text(encoding="utf-8", errors="replace")
                info["log_bytes"] = len(text)
                tail = text[-2000:]

                # Check for exit code
                exit_match = re.search(r"EXIT_CODE=(\d+)", tail)
                if exit_match:
                    code = int(exit_match.group(1))
                    info["exit_code"] = code
                    info["status"] = "ok" if code == 0 else "error"

                # Get progress summary
                summary_re = re.compile(r"(\d+) of (\d+) steps \((\d+)%\) done")
                for m in summary_re.finditer(tail):
                    info["progress"] = {
                        "completed": int(m.group(1)),
                        "total": int(m.group(2)),
                        "percent": int(m.group(3)),
                    }

                # Check for errors
                if "Error" in tail:
                    error_re = re.compile(r"Error in rule (\w+):")
                    errors = error_re.findall(tail)
                    if errors:
                        info["failed_rules"] = errors

                # If log is empty/tiny and screen is alive, check
                # snakemake's own log directory for progress hints
                if len(text) < 100 and info.get("screen_alive"):
                    info["hint"] = (
                        "Log is empty — likely in BIDS indexing or conda "
                        "environment setup. Check .snakemake/log/ for "
                        "snakemake's internal logs."
                    )
                    # Try to find snakemake's latest internal log
                    flow_log_dir = log_path.parent
                    sm_logs = sorted(flow_log_dir.glob("*.snakemake.log"))
                    if sm_logs:
                        latest = sm_logs[-1]
                        try:
                            sm_tail = latest.read_text(
                                encoding="utf-8", errors="replace"
                            )[-1000:]
                            info["snakemake_log_tail"] = sm_tail
                        except Exception:
                            pass

            except Exception:
                pass

        results.append(info)

    return {"runs": results}


def mcp_run_dashboard(root: Path) -> dict[str, Any]:
    """Rich summary of all tracked runs across flows.

    Returns aggregated stats: active, completed, failed runs per flow.
    """
    runs = _load_runs(root)
    if not runs:
        return {"flows": [], "totals": {"active": 0, "completed": 0, "failed": 0}}

    # Get fresh status
    status_result = mcp_run_status(root)
    all_runs = status_result.get("runs", [])

    # Aggregate by flow
    flow_stats: dict[str, dict[str, int]] = {}
    totals = {"active": 0, "completed": 0, "failed": 0}

    for run in all_runs:
        flow_key = run.get('flow', '?')
        if flow_key not in flow_stats:
            flow_stats[flow_key] = {"active": 0, "completed": 0, "failed": 0}

        status = run.get("status", "unknown")
        if status == "running":
            flow_stats[flow_key]["active"] += 1
            totals["active"] += 1
        elif status in ("ok", "finished"):
            flow_stats[flow_key]["completed"] += 1
            totals["completed"] += 1
        elif status == "error":
            flow_stats[flow_key]["failed"] += 1
            totals["failed"] += 1

    flows = [
        {"flow": k, **v}
        for k, v in sorted(flow_stats.items())
    ]

    return {
        "flows": flows,
        "totals": totals,
        "total_runs": len(all_runs),
    }


def mcp_run_kill(
    root: Path,
    run_id: str,
) -> dict[str, Any]:
    """Gracefully stop a running Snakemake screen session.

    Args:
        root: Project root.
        run_id: Run ID to stop.
    """
    import subprocess

    runs = _load_runs(root)
    target = None
    for run in runs:
        if run["id"] == run_id:
            target = run
            break

    if target is None:
        return {"error": f"Run ID {run_id!r} not found"}

    screen_name = target.get("screen", "")
    if not screen_name:
        return {"error": "No screen session recorded for this run"}

    try:
        subprocess.run(
            ["screen", "-S", screen_name, "-X", "quit"],
            check=True, capture_output=True,
        )
    except subprocess.CalledProcessError:
        return {"error": f"Failed to kill screen session {screen_name!r}"}

    # Update status
    target["status"] = "killed"
    _save_runs(root, runs)

    return {
        "killed": True,
        "run_id": run_id,
        "screen": screen_name,
    }
