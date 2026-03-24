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
