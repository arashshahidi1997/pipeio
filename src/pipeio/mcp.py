"""MCP tool functions for pipeio.

Called by projio's MCP server (``src/projio/mcp/pipeio.py``) to expose
pipeline management tools to AI agents.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


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


def mcp_nb_status(root: Path) -> dict[str, Any]:
    """Show notebook sync and publication status."""
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if not registry_path:
        return _NO_REGISTRY

    registry = PipelineRegistry.from_yaml(registry_path)
    flow_statuses: list[dict[str, Any]] = []

    for entry in registry.list_flows():
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
            nb_path = flow_root / nb.path if not Path(nb.path).is_absolute() else Path(nb.path)
            info: dict[str, Any] = {"name": Path(nb.path).stem}

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
            if mod_entry.doc_path:
                result["doc_exists"] = (root / mod_entry.doc_path).exists()
        results.append(result)

    return {"count": len(results), "results": results}


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

    registry = PipelineRegistry.scan(pipelines_dir, docs_dir=docs_dir)

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
                "has_config": f.config_path is not None,
                "has_docs": f.doc_path is not None,
                "mod_count": len(f.mods),
            }
            for f in flows
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
    nb_dir.mkdir(parents=True, exist_ok=True)

    nb_path = nb_dir / f"{name}.py"
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
            # Compute relative path from notebook dir to config
            try:
                rel = Path(config_path_str).resolve() if Path(
                    config_path_str
                ).is_absolute() else (root / config_path_str).resolve()
                rel_cfg = rel.relative_to(nb_dir.resolve())
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

    rel_nb = f"notebooks/{name}.py"
    existing_paths = {e.path for e in nb_cfg.entries}
    if rel_nb not in existing_paths:
        nb_cfg.entries.append(NotebookEntry(
            path=rel_nb,
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
) -> dict[str, Any]:
    """Sync a specific notebook (jupytext pair + convert).

    Args:
        root: Project root.
        pipe: Pipeline name.
        flow: Flow name.
        name: Notebook basename (without extension).
        formats: Which formats to produce (default: ['ipynb', 'myst']).
    """
    from pipeio.registry import PipelineRegistry

    if formats is None:
        formats = ["ipynb", "myst"]

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

    py_path = flow_dir / "notebooks" / f"{name}.py"
    if not py_path.exists():
        return {"error": f"Notebook not found: notebooks/{name}.py"}

    try:
        from pipeio.notebook.lifecycle import _require_jupytext, _jupytext
        _require_jupytext()
    except ImportError as exc:
        return {"error": str(exc)}

    generated: list[str] = []

    if "ipynb" in formats:
        ipynb_path = py_path.with_suffix(".ipynb")
        _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path))
        try:
            generated.append(str(ipynb_path.relative_to(root)))
        except ValueError:
            generated.append(str(ipynb_path))

    if "myst" in formats:
        myst_path = py_path.with_suffix(".md")
        _jupytext(py_path, "--to", "myst", "--output", str(myst_path))
        try:
            generated.append(str(myst_path.relative_to(root)))
        except ValueError:
            generated.append(str(myst_path))

    return {
        "synced": True,
        "source": str(py_path.relative_to(root)),
        "generated": generated,
        "formats": formats,
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
