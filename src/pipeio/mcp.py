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
    for candidate in (
        root / ".projio" / "pipeio" / "registry.yml",
        root / ".pipeio" / "registry.yml",
    ):
        if candidate.exists():
            return candidate
    return None


_NO_REGISTRY = {"error": "No pipeline registry found", "hint": "Run pipeio init"}


def _resolve_nb_path(flow_dir: Path, name: str) -> Path | None:
    """Resolve a notebook name to its .py path.

    Checks layouts in priority order:
    1. ``.src/`` layout: ``notebooks/.src/{name}.py`` (preferred)
    2. Flat layout: ``notebooks/{name}.py``
    3. Subdirectory layout: ``notebooks/{name}/{name}.py`` (legacy)
    4. Fall back to ``notebook.yml`` entry matching
    """
    # .src/ layout (preferred)
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


def mcp_flow_list(root: Path, pipe: str | None = None) -> dict[str, Any]:
    """List flows, optionally filtered by pipe."""
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY
    registry = PipelineRegistry.from_yaml(registry_path)
    flows = registry.list_flows(pipe=pipe)
    return {"flows": [f.model_dump() for f in flows]}


def mcp_flow_status(root: Path, pipe: str, flow: str) -> dict[str, Any]:
    """Show status of a specific flow."""
    from pipeio.config import FlowConfig
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    result: dict[str, Any] = {
        "pipe": entry.pipe,
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

    return result


def mcp_flow_deregister(
    root: Path,
    pipe: str,
    flow: str,
) -> dict[str, Any]:
    """Remove a flow from the pipeline registry.

    Only removes the registry entry — does NOT delete code, config, docs,
    or notebook files from the filesystem.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        removed = registry.remove(pipe, flow)
    except KeyError as exc:
        return {"error": str(exc)}

    registry.to_yaml(registry_path)

    # Persist to ignore list so rescan doesn't re-register
    ignore_path = registry_path.parent / "registry_ignore.yml"
    ignored: list[str] = []
    if ignore_path.exists():
        raw = yaml.safe_load(ignore_path.read_text(encoding="utf-8")) or {}
        ignored = raw.get("ignore", [])
    flow_key = f"{removed.pipe}/{removed.name}"
    if flow_key not in ignored:
        ignored.append(flow_key)
        ignore_path.write_text(
            yaml.safe_dump({"ignore": ignored}, default_flow_style=False),
            encoding="utf-8",
        )

    return {
        "deregistered": True,
        "pipe": removed.pipe,
        "flow": removed.name,
        "code_path": removed.code_path,
        "mods": list(removed.mods.keys()) if removed.mods else [],
        "note": "Registry entry removed. Added to registry_ignore.yml so rescan skips it.",
    }


def mcp_flow_fork(
    root: Path,
    pipe: str,
    flow: str,
    new_flow: str,
    new_pipe: str | None = None,
) -> dict[str, Any]:
    """Fork a flow: copy its code directory and register as a new flow.

    Creates a full copy of the flow's code (Snakefile, config, notebooks,
    scripts) under the new name.  The original flow is untouched.

    Args:
        root: Project root.
        pipe: Source pipeline name.
        flow: Source flow name.
        new_flow: Name for the forked flow.
        new_pipe: Target pipe (default: same as source).
    """
    import shutil

    from pipeio.registry import FlowEntry, PipelineRegistry

    if new_pipe is None:
        new_pipe = pipe

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)

    # Validate source exists
    try:
        source = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    # Check target doesn't conflict
    target_key = f"{new_pipe}/{new_flow}"
    if target_key in registry.flows:
        return {"error": f"Flow already exists: {target_key}"}

    # Resolve source code directory
    src_dir = Path(source.code_path)
    if not src_dir.is_absolute():
        src_dir = root / src_dir

    if not src_dir.exists():
        return {"error": f"Source code directory not found: {src_dir}"}

    # Compute target code directory (sibling of source, under new_pipe if changed)
    if new_pipe == pipe:
        dst_dir = src_dir.parent / new_flow
    else:
        dst_dir = src_dir.parent.parent / new_pipe / new_flow

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
        new_doc_path = source.doc_path.replace(
            f"{pipe}/{flow}", f"{new_pipe}/{new_flow}"
        )

    new_entry = FlowEntry(
        name=new_flow,
        pipe=new_pipe,
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
        "source": f"{pipe}/{flow}",
        "target": target_key,
        "code_path": new_code_path,
        "mods": list(new_entry.mods.keys()) if new_entry.mods else [],
    }


def mcp_nb_status(
    root: Path,
    pipe: str | None = None,
    flow: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Show notebook sync and publication status.

    Args:
        root: Project root.
        pipe: Filter to a specific pipeline (optional).
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
        # Apply pipe/flow filters
        if pipe and entry.pipe != pipe:
            continue
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
                "flow": f"{entry.pipe}/{entry.name}",
                "notebooks": notebooks,
            })

    return {"flows": flow_statuses}


def mcp_nb_update(
    root: Path,
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_root = Path(entry.config_path).parent if entry.config_path else None
    if not flow_root:
        return {"error": f"No config_path for {pipe}/{flow}"}
    if not flow_root.is_absolute():
        flow_root = root / flow_root

    nb_cfg_path = flow_root / "notebooks" / "notebook.yml"
    if not nb_cfg_path.exists():
        return {"error": f"No notebook.yml found for {pipe}/{flow}"}

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


def mcp_mod_list(root: Path, pipe: str, flow: str | None = None) -> dict[str, Any]:
    """List mods for a specific flow."""
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    return {
        "pipe": entry.pipe,
        "flow": entry.name,
        "mods": {name: mod.model_dump() for name, mod in entry.mods.items()},
    }


def _resolve_mod_doc_path(
    root: Path, pipe: str, flow: str, mod: str,
) -> tuple[str | None, bool]:
    """Resolve documentation path for a mod using docs tree conventions.

    Checks these locations in order:
    1. ``docs/pipelines/pipe-{pipe}/flow-{flow}/mod-{mod}/index.md``
    2. ``docs/explanation/pipelines/pipe-{pipe}/flow-{flow}/mod-{mod}/index.md``

    Returns (relative_path_or_None, exists_bool).
    """
    candidates = [
        root / "docs" / "pipelines" / f"pipe-{pipe}" / f"flow-{flow}" / f"mod-{mod}" / "index.md",
        root / "docs" / "explanation" / "pipelines" / f"pipe-{pipe}" / f"flow-{flow}" / f"mod-{mod}" / "index.md",
    ]
    for path in candidates:
        if path.exists():
            return str(path.relative_to(root)), True
    # Return the first convention path even if it doesn't exist yet
    return str(candidates[0].relative_to(root)), False


def mcp_mod_resolve(root: Path, modkeys: list[str]) -> dict[str, Any]:
    """Resolve modkeys (pipe-X_flow-Y_mod-Z) into metadata.

    Modkey format: ``pipe-<pipe>_flow-<flow>_mod-<mod>``
    """
    import re

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    pattern = re.compile(
        r"^(?:@)?pipe-(?P<pipe>[^_]+)_flow-(?P<flow>[^_]+)_mod-(?P<mod>.+)$"
    )

    results: list[dict[str, Any]] = []
    for raw_key in modkeys:
        key = raw_key.strip()
        m = pattern.match(key)
        if not m:
            results.append({"input": raw_key, "error": f"Invalid modkey format: {key!r}"})
            continue

        pipe, flow, mod = m.group("pipe"), m.group("flow"), m.group("mod")
        try:
            entry = registry.get(pipe, flow)
        except (KeyError, ValueError) as exc:
            results.append({"input": raw_key, "error": str(exc)})
            continue

        mod_entry = entry.mods.get(mod)
        result: dict[str, Any] = {
            "input": raw_key,
            "modkey": key,
            "pipe": pipe,
            "flow": flow,
            "mod": mod,
            "found": mod_entry is not None,
        }
        if mod_entry:
            result["meta"] = mod_entry.model_dump()
            # Resolve doc_path from both flow-local and docs tree conventions
            doc_path, doc_exists = _resolve_mod_doc_path(root, pipe, flow, mod)
            # Prefer flow-local doc_path from registry if it exists on disk
            if mod_entry.doc_path and (root / mod_entry.doc_path).exists():
                result["doc_path"] = mod_entry.doc_path
                result["doc_exists"] = True
            else:
                result["doc_path"] = doc_path
                result["doc_exists"] = doc_exists
        else:
            # Even for missing mods, provide the expected doc path
            doc_path, doc_exists = _resolve_mod_doc_path(root, pipe, flow, mod)
            result["doc_path"] = doc_path
            result["doc_exists"] = doc_exists
        results.append(result)

    return {"count": len(results), "results": results}


def mcp_mod_context(
    root: Path,
    pipe: str,
    flow: str | None = None,
    mod: str = "",
) -> dict[str, Any]:
    """Bundled read context for a single mod: rules, scripts, doc, config.

    Returns everything an agent needs to understand and work on a mod in one
    call.  Composes from existing internals — no new data model or cache.

    Args:
        root: Project root.
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    mod_entry = entry.mods.get(mod)
    if mod_entry is None:
        return {"error": f"Mod {mod!r} not found in {pipe}/{entry.name}"}

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

    # --- Doc: read mod documentation ---
    doc_content: str | None = None
    doc_path_str, doc_exists = _resolve_mod_doc_path(root, pipe, entry.name, mod)
    if mod_entry.doc_path and (root / mod_entry.doc_path).exists():
        doc_path_str = mod_entry.doc_path
        doc_exists = True
    if doc_exists and doc_path_str:
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
        "pipe": entry.pipe,
        "flow": entry.name,
        "mod": mod,
        "mod_meta": mod_entry.model_dump(),
        "rules": mod_rules,
        "scripts": scripts,
        "doc_path": doc_path_str,
        "doc": doc_content,
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
        "pipes": len(registry.list_pipes()),
        "flows": len(flows),
        "mods": total_mods,
        "flow_details": [
            {
                "pipe": f.pipe,
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
        Default: ``docs/pipelines/modkey.bib``.
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
        pipe = flow_entry.pipe
        flow = flow_entry.name
        for mod_name, mod_entry in sorted(flow_entry.mods.items()):
            modkey = f"pipe-{pipe}_flow-{flow}_mod-{mod_name}"
            doc_path, _ = _resolve_mod_doc_path(root, pipe, flow, mod_name)
            rules_str = ", ".join(mod_entry.rules) if mod_entry.rules else ""
            entry = (
                f"@misc{{{modkey},\n"
                f"  title     = {{{author} mod: pipe={pipe} flow={flow} mod={mod_name}}},\n"
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
        f"% Re-generate with: pipeio_modkey_bib()\n\n"
    )

    bib_content = header + "\n\n".join(entries) + "\n"

    # Write the file
    rel_path = output_path or "docs/pipelines/modkey.bib"
    out = root / rel_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(bib_content, encoding="utf-8")

    return {
        "path": rel_path,
        "entries": count,
        "flows": len(flows),
        "modkeys": [
            f"pipe-{f.pipe}_flow-{f.name}_mod-{m}"
            for f in flows
            for m in sorted(f.mods)
        ],
    }


def mcp_docs_collect(root: Path) -> dict[str, Any]:
    """Collect flow-local docs and notebook outputs into docs/pipelines/."""
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


def mcp_docs_nav(root: Path) -> dict[str, Any]:
    """Generate MkDocs nav YAML fragment for docs/pipelines/."""
    from pipeio.docs import docs_nav

    fragment = docs_nav(root)
    return {"nav_fragment": fragment}


def mcp_contracts_validate(root: Path) -> dict[str, Any]:
    """Validate I/O contracts for all flows in the registry."""
    from pipeio.contracts import validate_flow_contracts

    results = validate_flow_contracts(root)
    if not results:
        return _NO_REGISTRY

    flow_results = []
    for fv in results:
        flow_results.append({
            "flow": fv.flow_id,
            "valid": fv.ok,
            "passed": fv.passed,
            "warnings": fv.warnings,
            "errors": fv.errors,
        })

    all_valid = all(fv.ok for fv in results)
    return {
        "valid": all_valid,
        "flows": flow_results,
    }


def mcp_nb_create(
    root: Path,
    pipe: str,
    flow: str,
    name: str,
    kind: str = "investigate",
    description: str = "",
) -> dict[str, Any]:
    """Scaffold a new notebook for a flow.

    Creates a percent-format ``.py`` script with bootstrap cells
    (config load, registry groups) and registers it in ``notebook.yml``.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
        name: Notebook name (e.g. ``investigate_noise``).
        kind: Prefix convention (investigate, explore, demo).
        description: One-line purpose, injected as header comment.
    """
    from pipeio.config import FlowConfig
    from pipeio.notebook.config import NotebookConfig, NotebookEntry
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    # Resolve flow directory
    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    nb_dir = flow_dir / "notebooks"
    src_dir = nb_dir / ".src"
    src_dir.mkdir(parents=True, exist_ok=True)

    nb_path = src_dir / f"{name}.py"
    if nb_path.exists():
        return {"error": f"Notebook already exists: {nb_path.relative_to(root)}"}

    # Load flow config for registry groups
    groups: list[str] = []
    config_path_str = entry.config_path
    if config_path_str:
        cfg_path = Path(config_path_str)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        if cfg_path.exists():
            try:
                cfg = FlowConfig.from_yaml(cfg_path)
                groups = cfg.groups()
            except Exception:
                pass

    # Generate percent-format .py
    lines: list[str] = []
    lines.append("# ---")
    lines.append(f"# jupyter:")
    lines.append(f"#   jupytext:")
    lines.append(f"#     text_representation:")
    lines.append(f"#       format_name: percent")
    lines.append("# ---")
    lines.append("")
    lines.append(f'# %% [markdown]')
    desc_text = description or f"{kind.title()} notebook for {pipe}/{flow}"
    lines.append(f"# # {name.replace('_', ' ').title()}")
    lines.append(f"#")
    lines.append(f"# {desc_text}")
    lines.append("")
    lines.append("# %% [markdown]")
    lines.append("# ## Setup")
    lines.append("")
    lines.append("# %%")
    lines.append("from pathlib import Path")
    lines.append("")
    if config_path_str:
        rel_cfg = Path(config_path_str)
        if not rel_cfg.is_absolute():
            # Compute relative path from .src/ dir to config
            try:
                rel = Path(config_path_str).resolve() if Path(
                    config_path_str
                ).is_absolute() else (root / config_path_str).resolve()
                rel_cfg = rel.relative_to(src_dir.resolve())
            except ValueError:
                rel_cfg = Path(config_path_str)
        lines.append(f'config_path = Path("{rel_cfg}")')
    lines.append("")

    if groups:
        lines.append(f"# Registry groups: {', '.join(groups)}")
        lines.append("")

    lines.append("# %% [markdown]")
    lines.append("# ## Analysis")
    lines.append("")
    lines.append("# %%")
    lines.append("")

    nb_path.write_text("\n".join(lines), encoding="utf-8")

    # Register in notebook.yml
    nb_cfg_path = nb_dir / "notebook.yml"
    if nb_cfg_path.exists():
        try:
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
        except Exception:
            nb_cfg = NotebookConfig()
    else:
        nb_cfg = NotebookConfig()

    rel_nb = f"notebooks/.src/{name}.py"
    existing_paths = {e.path for e in nb_cfg.entries}
    if rel_nb not in existing_paths:
        nb_cfg.entries.append(NotebookEntry(
            path=rel_nb,
            kind=kind,
            description=description,
            status="active",
            pair_ipynb=True,
            pair_myst=True,
            publish_myst=True,
        ))
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
        "pipe": pipe,
        "flow": flow,
        "name": name,
        "kind": kind,
        "registry_groups": groups,
        "notebook_yml_updated": rel_nb not in existing_paths,
    }


def mcp_nb_sync(
    root: Path,
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
        if py_path is None or not py_path.with_suffix(".ipynb").exists():
            return {"error": f"Paired notebook not found: {name}.ipynb"}

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
    pipe: str,
    flow: str,
    direction: str = "py2nb",
    force: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Batch-sync all notebooks in a flow.

    Args:
        root: Project root.
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if not nb_cfg_path.exists():
        return {"error": f"No notebook.yml found for {pipe}/{flow}"}

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
        "pipe": pipe,
        "flow": flow,
        "direction": direction,
        "total": len(results),
        "synced": len(synced),
        "skipped": len(skipped),
        "results": results,
    }


def mcp_nb_diff(
    root: Path,
    pipe: str,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Show sync state between .py and paired .ipynb for a notebook.

    Returns which file is newer, whether they're in sync, and the
    recommended sync direction. Useful before deciding whether to
    sync and in which direction.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
        name: Notebook basename (without extension).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
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
    pipe: str | None = None,
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
        pipe: Filter to a specific pipeline (optional).
        flow: Filter to a specific flow (optional).
        sync: If True, sync py→ipynb before linking (default False).
        python_bin: Python binary where jupytext is installed (optional).
    """
    from pipeio.notebook.lifecycle import nb_lab

    result = nb_lab(
        root, pipe=pipe, flow=flow,
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
    pipe: str,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Read a notebook's .py content and return it with metadata.

    Combines file content, sync state, structural analysis, and config
    metadata (status, kernel, mod) in a single call.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
        name: Notebook basename (without extension).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
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
    pipe: str,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Publish a notebook's myst markdown to the docs tree.

    Copies the ``.md`` (myst) file from the flow's notebooks directory
    to ``docs/pipelines/<pipe>/<flow>/notebooks/nb-<name>.md``.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
        name: Notebook basename (without extension).
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    myst_src = flow_dir / "notebooks" / f"{name}.md"
    if not myst_src.exists():
        return {
            "error": f"MyST file not found: notebooks/{name}.md",
            "hint": "Run pipeio_nb_sync first to generate the .md file.",
        }

    import shutil

    dest_dir = root / "docs" / "pipelines" / pipe / flow / "notebooks"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"nb-{name}.md"
    shutil.copy2(myst_src, dest)

    try:
        result_path = str(dest.relative_to(root))
    except ValueError:
        result_path = str(dest)

    return {
        "published": result_path,
        "source": str(myst_src.relative_to(root)),
        "pipe": pipe,
        "flow": flow,
        "name": name,
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


def mcp_rule_list(root: Path, pipe: str, flow: str | None = None) -> dict[str, Any]:
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
        entry = registry.get(pipe, flow)
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
        "pipe": entry.pipe,
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
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
        "pipe": entry.pipe,
        "flow": entry.name,
        "stub": stub_text,
    }


def mcp_rule_insert(
    root: Path,
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
        "pipe": entry.pipe,
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
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
        "pipe": entry.pipe,
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


def mcp_config_read(root: Path, pipe: str, flow: str | None = None) -> dict[str, Any]:
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
        entry = reg.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if not entry.config_path:
        return {"error": f"No config_path registered for {pipe}/{entry.name}"}

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
        "pipe": entry.pipe,
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
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = reg.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if not entry.config_path:
        return {"error": f"No config_path registered for {pipe}/{entry.name}"}

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
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = reg.get(pipe, flow)
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
        output_dir = f"derivatives/{pipe}"
    config["output_dir"] = output_dir
    config["output_registry"] = (
        f"{output_dir}/pipe-{entry.pipe}_flow-{entry.name}_registry.yml"
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
        "pipe": entry.pipe,
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
            "pipes": len(registry.list_pipes()),
            "flows": len(flows),
            "mods": total_mods,
        },
    }


def mcp_nb_analyze(
    root: Path,
    pipe: str,
    flow: str,
    name: str,
) -> dict[str, Any]:
    """Analyze a notebook's static structure.

    Parses the percent-format ``.py`` file and returns structured metadata:
    imports, RunCard @dataclass fields, PipelineContext usage, section headers,
    and cogpy function calls.

    Args:
        root: Project root.
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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

    # Create doc stub
    docs_dir = flow_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    doc_path = docs_dir / f"mod-{mod}.md"
    created_doc = False
    if not doc_path.exists():
        doc_lines = [
            "---",
            f"mod: {mod}",
            f"pipe: {pipe}",
            f"flow: {entry.name}",
            "---",
            "",
            f"# {mod.replace('_', ' ').title()}",
            "",
            desc_text,
            "",
            "## Rules",
            "",
            "## Parameters",
            "",
            "## Assumptions",
            "",
        ]
        doc_path.write_text("\n".join(doc_lines), encoding="utf-8")
        created_doc = True

    try:
        script_rel = str(script_path.relative_to(root))
        doc_rel = str(doc_path.relative_to(root))
    except ValueError:
        script_rel = str(script_path)
        doc_rel = str(doc_path)

    return {
        "created_script": script_rel,
        "created_doc": doc_rel if created_doc else None,
        "pipe": pipe,
        "flow": entry.name,
        "mod": mod,
        "seeded_from": from_notebook if nb_imports else None,
        "io_wiring": has_io,
        "pipeline_context": use_pipeline_context,
    }


# ---------------------------------------------------------------------------
# MCP tools: notebook execution
# ---------------------------------------------------------------------------


def mcp_nb_exec(
    root: Path,
    pipe: str,
    flow: str,
    name: str,
    params: dict[str, Any] | None = None,
    timeout: int = 600,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Execute a notebook via papermill with optional parameter overrides.

    Syncs the notebook first (py → ipynb), then executes via papermill.
    Returns structured result with status, errors, output path, and elapsed time.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
        name: Notebook basename (without extension).
        params: RunCard parameter overrides (injected into papermill).
        timeout: Cell execution timeout in seconds (default 600).
        python_bin: Python binary for execution (optional).
    """
    import subprocess
    import time

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    flow_dir = Path(entry.code_path)
    if not flow_dir.is_absolute():
        flow_dir = root / flow_dir

    py_path = _resolve_nb_path(flow_dir, name)
    if py_path is None:
        return {"error": f"Notebook not found: {name}"}

    ipynb_path = py_path.with_suffix(".ipynb")

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
        _require_jupytext(python_bin=python_bin)
        kernel_args: tuple[str, ...] = ("--set-kernel", kernel) if kernel else ()
        _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path),
                  *kernel_args, python_bin=python_bin)
    except (ImportError, Exception) as exc:
        return {"error": f"Sync failed: {exc}"}

    # Build papermill command
    output_path = ipynb_path.with_name(f"{name}_executed.ipynb")
    python = python_bin or "python"
    cmd = [python, "-m", "papermill", str(ipynb_path), str(output_path),
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
            "pipe": pipe,
            "flow": entry.name,
            "name": name,
        }
    except FileNotFoundError:
        return {"error": f"papermill not found. Install with: pip install papermill"}

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
            "pipe": pipe,
            "flow": entry.name,
            "name": name,
        }

    return {
        "status": "ok",
        "elapsed_seconds": elapsed,
        "output_path": output_rel,
        "pipe": pipe,
        "flow": entry.name,
        "name": name,
        "params_injected": list(params.keys()) if params else [],
    }


# ---------------------------------------------------------------------------
# MCP tools: snakemake native DAG export and report
# ---------------------------------------------------------------------------


def mcp_dag_export(
    root: Path,
    pipe: str,
    flow: str | None = None,
    graph_type: str = "rulegraph",
    output_format: str = "dot",
    snakemake_cmd: list[str] | None = None,
) -> dict[str, Any]:
    """Export rule/job DAG via snakemake's native graph output.

    Shells out to ``snakemake --rulegraph`` (or ``--dag``, ``--d3dag``)
    and optionally converts dot output to SVG via graphviz.

    Args:
        root: Project root.
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
        try:
            svg_result = subprocess.run(
                ["dot", "-Tsvg"], input=graph_output, capture_output=True,
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
                "error": "graphviz 'dot' not found — install with: apt install graphviz",
                "dot": graph_output,
            }

    return {
        "pipe": entry.pipe,
        "flow": entry.name,
        "graph_type": graph_type,
        "format": "json" if graph_type == "d3dag" else output_format,
        "output": graph_output,
    }


def mcp_report(
    root: Path,
    pipe: str,
    flow: str | None = None,
    output_path: str = "",
    target: str = "",
    snakemake_cmd: list[str] | None = None,
) -> dict[str, Any]:
    """Generate a snakemake HTML report for a flow.

    Uses ``snakemake --report`` to produce a self-contained HTML report
    with runtime statistics, provenance, and annotated outputs.

    When ``target`` is specified (e.g. ``"report"``), snakemake runs that
    target rule first, which is useful for flows that define a ``rule report``
    filtering to existing outputs only.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name (optional for single-flow pipes).
        output_path: Where to write the report (relative to root).
            Defaults to ``derivatives/{pipe}/report.html``.
        target: Target rule to run before generating the report (e.g.
            ``"report"``). If empty, ``--report`` runs against existing
            metadata only.
        snakemake_cmd: Command tokens to invoke snakemake.
    """
    import subprocess

    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    try:
        entry = registry.get(pipe, flow)
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
        output_path = f"derivatives/{entry.pipe}/report.html"
    report_abs = root / output_path
    report_abs.parent.mkdir(parents=True, exist_ok=True)

    # Build command
    cmd = [
        *snake_base,
        "--snakefile", str(snakefile),
        "--directory", str(flow_dir),
        "--report", str(report_abs),
    ]
    if target:
        cmd.append(target)

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
        "pipe": entry.pipe,
        "flow": entry.name,
        "report_path": output_path,
        "exists": report_abs.exists(),
        "size_kb": round(report_abs.stat().st_size / 1024, 1) if report_abs.exists() else 0,
    }


# ---------------------------------------------------------------------------
# MCP tools: completion tracking
# ---------------------------------------------------------------------------


def mcp_completion(
    root: Path,
    pipe: str,
    flow: str | None = None,
    mod: str | None = None,
) -> dict[str, Any]:
    """Check session completion by comparing expected outputs against filesystem.

    For each registry group and member, expands the output directory to find
    existing files, then reports which sessions are complete, partial, or missing.

    Args:
        root: Project root.
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    if not entry.config_path:
        return {"error": f"No config_path for {pipe}/{entry.name}"}

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
        "pipe": entry.pipe,
        "flow": entry.name,
        "output_dir": cfg.output_dir,
        "groups": group_results,
    }


# ---------------------------------------------------------------------------
# MCP tools: target path resolution
# ---------------------------------------------------------------------------


def mcp_target_paths(
    root: Path,
    pipe: str,
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
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
    except (KeyError, ValueError) as exc:
        return {"error": str(exc)}

    try:
        ctx = PipelineContext.from_registry(
            pipe, flow, root=root, registry=registry,
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
            "pipe": entry.pipe,
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
                "pipe": entry.pipe,
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
                "pipe": entry.pipe,
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
            "pipe": entry.pipe,
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
        "pipe": entry.pipe,
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
    pipe: str | None = None,
    flow: str | None = None,
) -> dict[str, Any]:
    """Map output_registry → input_registry chains across flows.

    For each flow that declares an ``input_registry`` in its config, finds
    which other flow's ``output_registry`` matches.  Detects stale or broken
    references.

    Args:
        root: Project root.
        pipe: Filter by pipeline name (optional).
        flow: Filter by flow name (optional).
    """
    import yaml
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    flows = registry.list_flows(pipe=pipe)

    # Build flow metadata with input/output registry paths
    flow_meta: list[dict[str, Any]] = []
    for entry in flows:
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

        flow_meta.append({
            "pipe": entry.pipe,
            "flow": entry.name,
            "flow_id": f"{entry.pipe}/{entry.name}",
            "input_dir": raw.get("input_dir", ""),
            "input_registry": raw.get("input_registry", ""),
            "output_dir": raw.get("output_dir", ""),
            "output_registry": raw.get("output_registry", ""),
        })

    # Build output_registry → flow mapping
    output_map: dict[str, str] = {}
    for fm in flow_meta:
        if fm["output_registry"]:
            output_map[fm["output_registry"]] = fm["flow_id"]

    # Build chains: for each flow with input_registry, find its source
    chains: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []

    for fm in flow_meta:
        if not fm["input_registry"]:
            continue

        input_reg = fm["input_registry"]
        source_flow = output_map.get(input_reg)

        chain_entry: dict[str, Any] = {
            "consumer": fm["flow_id"],
            "input_registry": input_reg,
            "producer": source_flow,
        }

        if source_flow:
            # Check if the directory exists
            dir_exists = (root / input_reg).exists() if input_reg else False
            chain_entry["dir_exists"] = dir_exists
            chains.append(chain_entry)
        else:
            # Input registry not produced by any known flow
            chain_entry["status"] = "unresolved"
            chain_entry["hint"] = "No flow outputs to this registry"
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
    pipe: str,
    flow: str | None = None,
    run_id: str | None = None,
    log_path: str | None = None,
) -> dict[str, Any]:
    """Extract structured data from Snakemake log files.

    Parses logs to find completed rules with timing, failed rules with error
    summaries, resource warnings, and missing inputs.

    Args:
        root: Project root.
        pipe: Pipeline name.
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
        entry = registry.get(pipe, flow)
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
        "pipe": entry.pipe,
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
    pipe: str,
    flow: str | None = None,
    targets: list[str] | None = None,
    cores: int = 1,
    dryrun: bool = False,
    extra_args: list[str] | None = None,
    snakemake_cmd: list[str] | None = None,
    wildcards: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Launch Snakemake in a detached screen session.

    State is tracked in ``.pipeio/runs.json``.

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name (optional for single-flow pipes).
        targets: Snakemake target rules (optional).
        cores: Number of cores (default 1).
        dryrun: If True, pass ``-n`` for a dry run.
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
        entry = registry.get(pipe, flow)
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
    ]
    if dryrun:
        snake_cmd.append("-n")
    if targets:
        snake_cmd.extend(targets)
    if wildcards:
        for key, value in wildcards.items():
            snake_cmd.extend([f"--filter-{key}", str(value)])
    if extra_args:
        snake_cmd.extend(extra_args)

    screen_name = f"pipeio-{entry.pipe}-{entry.name}-{run_id}"
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
        "pipe": entry.pipe,
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
        "pipe": entry.pipe,
        "flow": entry.name,
        "dryrun": dryrun,
    }


def mcp_run_status(
    root: Path,
    run_id: str | None = None,
    pipe: str | None = None,
    flow: str | None = None,
) -> dict[str, Any]:
    """Query progress of running or recent Snakemake runs.

    Checks screen sessions and parses the tail of log files for progress.

    Args:
        root: Project root.
        run_id: Specific run ID to query (optional).
        pipe: Filter by pipeline (optional).
        flow: Filter by flow (optional).
    """
    import subprocess

    runs = _load_runs(root)
    if not runs:
        return {"runs": [], "message": "No runs recorded"}

    # Filter
    if run_id:
        runs = [r for r in runs if r["id"] == run_id]
    if pipe:
        runs = [r for r in runs if r.get("pipe") == pipe]
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
        flow_key = f"{run.get('pipe', '?')}/{run.get('flow', '?')}"
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
