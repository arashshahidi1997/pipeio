"""Notebook lifecycle operations: pair, sync, exec, publish, status.

Requires ``pipeio[notebook]`` (jupytext, nbconvert) for pair/sync/exec/publish.
Status works without any optional dependencies.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def find_notebook_configs(root: Path) -> list[tuple[Path, Any]]:
    """Return ``[(flow_root, NotebookConfig)]`` for every ``notebooks/notebook.yml`` under *root*."""
    from pipeio.notebook.config import NotebookConfig

    results = []
    for nb_yml in sorted(root.rglob("notebooks/notebook.yml")):
        flow_root = nb_yml.parent.parent
        try:
            cfg = NotebookConfig.from_yaml(nb_yml)
            results.append((flow_root, cfg))
        except Exception:
            pass
    return results


def _is_percent_format(py_path: Path) -> bool:
    """Return True if *py_path* looks like a jupytext percent-format notebook."""
    try:
        head = py_path.read_text(encoding="utf-8", errors="ignore")[:4096]
        return "# %%" in head
    except Exception:
        return False


def nb_scan(root: Path, *, register: bool = False) -> list[dict[str, Any]]:
    """Scan for percent-format .py notebooks and compare against notebook.yml.

    Walks every ``notebooks/`` directory under *root*, finds ``.py`` files
    containing ``# %%`` cell markers, and checks whether they are registered
    in the corresponding ``notebook.yml``.

    Parameters
    ----------
    root : Path
        Project root.
    register : bool
        If True, auto-register unregistered notebooks into ``notebook.yml``
        with sensible defaults (pair_ipynb=True, pair_myst=True, status=draft).

    Returns
    -------
    List of dicts, one per discovered notebook:
        ``name``, ``py_path``, ``flow_root``, ``registered`` (bool),
        ``newly_registered`` (bool, only when register=True).
    """
    from pipeio.notebook.config import NotebookConfig, NotebookEntry

    results: list[dict[str, Any]] = []

    # Find all notebooks/ directories
    seen_nb_dirs: set[Path] = set()
    for py_file in sorted(root.rglob("notebooks/*.py")):
        if not _is_percent_format(py_file):
            continue
        nb_dir = py_file.parent
        flow_root = nb_dir.parent
        seen_nb_dirs.add(nb_dir)

        # Also check subdirectory pattern: notebooks/<name>/<name>.py
        # (already caught by the glob)

    # Also check notebooks/<subdir>/<name>.py pattern
    for py_file in sorted(root.rglob("notebooks/**/*.py")):
        if not _is_percent_format(py_file):
            continue
        # flow_root is the parent of the notebooks/ dir
        nb_dir_candidate = py_file.parent
        while nb_dir_candidate.name != "notebooks" and nb_dir_candidate != root:
            nb_dir_candidate = nb_dir_candidate.parent
        if nb_dir_candidate.name == "notebooks":
            seen_nb_dirs.add(nb_dir_candidate)

    # Now process each notebooks/ dir
    processed_files: set[Path] = set()
    for nb_dir in sorted(seen_nb_dirs):
        flow_root = nb_dir.parent
        cfg_path = nb_dir / "notebook.yml"

        # Load existing config or create empty
        if cfg_path.exists():
            try:
                cfg = NotebookConfig.from_yaml(cfg_path)
            except Exception:
                cfg = NotebookConfig()
        else:
            cfg = NotebookConfig()

        registered_paths = {Path(e.path).resolve() for e in cfg.entries
                           if (flow_root / e.path).exists()}
        registered_stems = {Path(e.path).stem for e in cfg.entries}

        modified = False

        # Scan .py files in this notebooks/ dir (flat and nested)
        for py_file in sorted(nb_dir.rglob("*.py")):
            if not _is_percent_format(py_file) or py_file in processed_files:
                continue
            processed_files.add(py_file)

            try:
                rel_path = str(py_file.relative_to(flow_root))
            except ValueError:
                rel_path = str(py_file)

            is_registered = (py_file.stem in registered_stems
                             or py_file.resolve() in registered_paths)

            entry_info: dict[str, Any] = {
                "name": py_file.stem,
                "py_path": str(py_file),
                "rel_path": rel_path,
                "flow_root": str(flow_root),
                "registered": is_registered,
            }

            if not is_registered and register:
                # Prefer .src/ layout for new registrations
                if ".src" not in rel_path:
                    src_rel = rel_path.replace("notebooks/", "notebooks/.src/", 1)
                else:
                    src_rel = rel_path
                new_entry = NotebookEntry(
                    path=src_rel if ".src" in src_rel else rel_path,
                    status="draft",
                    pair_ipynb=True,
                    pair_myst=True,
                    publish_myst=True,
                )
                cfg.entries.append(new_entry)
                modified = True
                entry_info["newly_registered"] = True

            results.append(entry_info)

        if modified:
            cfg.to_yaml(cfg_path)

    return results


# ---------------------------------------------------------------------------
# Status (no optional deps)
# ---------------------------------------------------------------------------

def nb_status(root: Path) -> list[dict[str, Any]]:
    """Return status records for every notebook under *root*.

    Each record has keys:
    ``name``, ``flow_root``, ``py_exists``, ``ipynb_exists``,
    ``myst_exists``, ``synced``, ``executed``.
    """
    statuses: list[dict[str, Any]] = []

    for flow_root, cfg in find_notebook_configs(root):
        for entry in cfg.entries:
            py_path = flow_root / entry.path
            _ipynb, _myst = _nb_output_paths(py_path)
            ipynb_path = _ipynb if entry.pair_ipynb else None
            myst_path = _myst if entry.pair_myst else None

            synced = True
            if py_path.exists():
                py_mtime = py_path.stat().st_mtime
                if ipynb_path is not None:
                    synced = synced and (ipynb_path.exists() and ipynb_path.stat().st_mtime >= py_mtime)
                if myst_path is not None:
                    synced = synced and (myst_path.exists() and myst_path.stat().st_mtime >= py_mtime)
            else:
                synced = False

            executed = False
            if ipynb_path is not None and ipynb_path.exists():
                try:
                    nb_data = json.loads(ipynb_path.read_text(encoding="utf-8"))
                    code_cells = [c for c in nb_data.get("cells", []) if c.get("cell_type") == "code"]
                    executed = any(
                        c.get("outputs") or c.get("execution_count")
                        for c in code_cells
                    )
                except Exception:
                    pass

            statuses.append({
                "name": py_path.stem,
                "flow_root": str(flow_root),
                "py_exists": py_path.exists(),
                "ipynb_exists": ipynb_path.exists() if ipynb_path is not None else None,
                "myst_exists": myst_path.exists() if myst_path is not None else None,
                "synced": synced,
                "executed": executed,
            })

    return statuses


# ---------------------------------------------------------------------------
# Pair
# ---------------------------------------------------------------------------

def nb_pair(root: Path, *, force: bool = False) -> list[str]:
    """Pair ``.py`` notebooks with ``.ipynb`` / ``.myst`` using jupytext.

    Only creates pairs that are missing (or all pairs when *force* is True).
    Returns a list of paths that were created.
    """
    _require_jupytext()
    created: list[str] = []

    for flow_root, cfg in find_notebook_configs(root):
        for entry in cfg.entries:
            py_path = flow_root / entry.path
            if not py_path.exists():
                continue

            ipynb_path, myst_path = _nb_output_paths(py_path)

            if entry.pair_ipynb:
                if force or not ipynb_path.exists():
                    _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path))
                    created.append(str(ipynb_path))

            if entry.pair_myst:
                myst_path.parent.mkdir(parents=True, exist_ok=True)
                if force or not myst_path.exists():
                    _jupytext(py_path, "--to", "myst", "--output", str(myst_path))
                    created.append(str(myst_path))

    return created


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def nb_sync(root: Path) -> list[str]:
    """Sync paired notebooks that are older than their ``.py`` source.

    Returns a list of paths that were updated.
    """
    _require_jupytext()
    updated: list[str] = []

    for flow_root, cfg in find_notebook_configs(root):
        for entry in cfg.entries:
            py_path = flow_root / entry.path
            if not py_path.exists():
                continue
            py_mtime = py_path.stat().st_mtime

            ipynb_path, myst_path = _nb_output_paths(py_path)

            if entry.pair_ipynb:
                if ipynb_path.exists() and ipynb_path.stat().st_mtime < py_mtime:
                    _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path))
                    updated.append(str(ipynb_path))

            if entry.pair_myst:
                myst_path.parent.mkdir(parents=True, exist_ok=True)
                if myst_path.exists() and myst_path.stat().st_mtime < py_mtime:
                    _jupytext(py_path, "--to", "myst", "--output", str(myst_path))
                    updated.append(str(myst_path))

    return updated


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

def nb_exec(root: Path) -> list[str]:
    """Execute all ``.ipynb`` notebooks in-place via ``jupyter nbconvert``.

    Returns a list of paths that were executed.
    """
    _require_nbconvert()
    executed: list[str] = []

    for flow_root, cfg in find_notebook_configs(root):
        for entry in cfg.entries:
            if not entry.pair_ipynb:
                continue
            py_path = flow_root / entry.path
            ipynb_path, _ = _nb_output_paths(py_path)
            if not ipynb_path.exists():
                continue
            _nbconvert_exec(ipynb_path)
            executed.append(str(ipynb_path))

    return executed


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

def nb_publish(root: Path) -> list[str]:
    """Publish notebooks to the docs directory configured in ``notebook.yml``.

    - ``publish_html=True`` entries are converted to HTML via nbconvert.
    - ``publish_myst=True`` entries are copied as ``.md``.

    Returns a list of published output paths.
    """
    published: list[str] = []

    for flow_root, cfg in find_notebook_configs(root):
        if not cfg.publish.docs_dir:
            continue
        docs_dir = root / cfg.publish.docs_dir
        docs_dir.mkdir(parents=True, exist_ok=True)

        for entry in cfg.entries:
            py_path = flow_root / entry.path
            name = py_path.stem
            ipynb_path, myst_path = _nb_output_paths(py_path)

            if entry.publish_html:
                _require_nbconvert()
                if ipynb_path.exists():
                    out = docs_dir / f"{cfg.publish.prefix}{name}.html"
                    _nbconvert_html(ipynb_path, out)
                    published.append(str(out))

            if entry.publish_myst:
                _require_jupytext()
                if myst_path.exists():
                    out = docs_dir / f"{cfg.publish.prefix}{name}.md"
                    shutil.copy2(myst_path, out)
                    published.append(str(out))

    return published


# ---------------------------------------------------------------------------
# Single-notebook sync (bidirectional)
# ---------------------------------------------------------------------------

def nb_sync_one(
    py_path: Path,
    *,
    direction: str = "py2nb",
    formats: list[str] | None = None,
    force: bool = False,
    kernel: str = "",
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Sync a single notebook, optionally in either direction.

    Parameters
    ----------
    py_path : Path
        The percent-format ``.py`` source file.
    direction : str
        ``"py2nb"`` (default) — regenerate ``.ipynb`` / ``.md`` from ``.py``.
        ``"nb2py"`` — update ``.py`` from the paired ``.ipynb``.
    formats : list[str] | None
        Which paired formats to produce/consume (default ``["ipynb", "myst"]``).
        Only used for ``py2nb`` direction.
    force : bool
        If False (default), skip files that are already up-to-date (mtime check).
    kernel : str
        Jupyter kernel name to embed in the ``.ipynb`` (e.g. ``"cogpy"``).
        Passed as ``--set-kernel`` to jupytext.  Empty string = no override.
    python_bin : str | None
        Python binary where jupytext is installed (optional).

    Returns
    -------
    dict with keys: synced (bool), source, generated/updated (list[str]),
    direction, skipped (bool if nothing needed).
    """
    if formats is None:
        formats = ["ipynb", "myst"]

    _require_jupytext(python_bin=python_bin)

    if direction == "nb2py":
        return _sync_nb2py(py_path, force=force, python_bin=python_bin)
    elif direction == "py2nb":
        return _sync_py2nb(py_path, formats=formats, force=force, kernel=kernel, python_bin=python_bin)
    else:
        return {"error": f"Unknown direction: {direction!r}. Use 'py2nb' or 'nb2py'."}


def _nb_output_paths(py_path: Path) -> tuple[Path, Path]:
    """Compute ipynb and myst output paths from a .py source path.

    Layout-aware: if ``.py`` is in a ``.src/`` directory, ``.ipynb`` goes
    to the parent workspace dir and ``.md`` goes to ``.myst/``.

    Supports both flat and workspace layouts::

        notebooks/.src/foo.py        → notebooks/foo.ipynb, notebooks/.myst/foo.md
        notebooks/explore/.src/foo.py → notebooks/explore/foo.ipynb, notebooks/explore/.myst/foo.md
        notebooks/demo/.src/foo.py   → notebooks/demo/foo.ipynb, notebooks/demo/.myst/foo.md

    Otherwise, outputs are siblings of the ``.py`` file.
    """
    name = py_path.stem
    if py_path.parent.name == ".src":
        workspace_dir = py_path.parent.parent  # notebooks/ or notebooks/explore/
        ipynb_path = workspace_dir / f"{name}.ipynb"
        myst_dir = workspace_dir / ".myst"
        myst_path = myst_dir / f"{name}.md"
    else:
        ipynb_path = py_path.with_suffix(".ipynb")
        myst_path = py_path.with_suffix(".md")
    return ipynb_path, myst_path


def _sync_py2nb(
    py_path: Path,
    *,
    formats: list[str],
    force: bool = False,
    kernel: str = "",
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Sync .py → .ipynb / .myst."""
    if not py_path.exists():
        return {"error": f"Source not found: {py_path}"}

    py_mtime = py_path.stat().st_mtime
    generated: list[str] = []
    kernel_args: tuple[str, ...] = ("--set-kernel", kernel) if kernel else ()
    ipynb_path, myst_path = _nb_output_paths(py_path)

    if "ipynb" in formats:
        if force or not ipynb_path.exists() or ipynb_path.stat().st_mtime < py_mtime:
            _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path),
                       *kernel_args, python_bin=python_bin)
            generated.append(str(ipynb_path))

    if "myst" in formats:
        myst_path.parent.mkdir(parents=True, exist_ok=True)
        if force or not myst_path.exists() or myst_path.stat().st_mtime < py_mtime:
            _jupytext(py_path, "--to", "myst", "--output", str(myst_path), python_bin=python_bin)
            generated.append(str(myst_path))

    return {
        "synced": bool(generated),
        "skipped": not generated,
        "direction": "py2nb",
        "source": str(py_path),
        "generated": generated,
        **({"kernel": kernel} if kernel else {}),
    }


def _sync_nb2py(
    py_path: Path,
    *,
    force: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Sync .ipynb → .py (reverse sync for human edits)."""
    ipynb_path, _ = _nb_output_paths(py_path)
    if not ipynb_path.exists():
        return {"error": f"Paired notebook not found: {ipynb_path}"}

    if not force and py_path.exists():
        if py_path.stat().st_mtime >= ipynb_path.stat().st_mtime:
            return {
                "synced": False,
                "skipped": True,
                "direction": "nb2py",
                "source": str(ipynb_path),
                "reason": ".py is already newer than .ipynb",
            }

    py_path.parent.mkdir(parents=True, exist_ok=True)
    _jupytext(ipynb_path, "--to", "py:percent", "--output", str(py_path), python_bin=python_bin)

    return {
        "synced": True,
        "skipped": False,
        "direction": "nb2py",
        "source": str(ipynb_path),
        "updated": [str(py_path)],
    }


# ---------------------------------------------------------------------------
# Single-notebook diff (change detection)
# ---------------------------------------------------------------------------

def nb_diff(py_path: Path) -> dict[str, Any]:
    """Compare sync state between ``.py`` and its paired ``.ipynb``.

    Returns a dict describing which file is newer, whether they're in sync,
    and the recommended sync direction.
    """
    ipynb_path, _ = _nb_output_paths(py_path)

    result: dict[str, Any] = {
        "py_path": str(py_path),
        "ipynb_path": str(ipynb_path),
        "py_exists": py_path.exists(),
        "ipynb_exists": ipynb_path.exists(),
    }

    if not py_path.exists() and not ipynb_path.exists():
        result["status"] = "missing"
        result["recommendation"] = "Neither file exists"
        return result

    if py_path.exists() and not ipynb_path.exists():
        result["status"] = "unpaired"
        result["recommendation"] = "Run sync direction=py2nb to create .ipynb"
        return result

    if not py_path.exists() and ipynb_path.exists():
        result["status"] = "orphaned_ipynb"
        result["recommendation"] = "Run sync direction=nb2py to create .py"
        return result

    py_mtime = py_path.stat().st_mtime
    ipynb_mtime = ipynb_path.stat().st_mtime
    result["py_mtime"] = py_mtime
    result["ipynb_mtime"] = ipynb_mtime

    # Use 2-second tolerance to avoid mtime race from jupytext
    # (jupytext writes .ipynb then touches .py metadata, creating a small gap)
    _MTIME_TOLERANCE = 2.0

    if py_mtime - ipynb_mtime > _MTIME_TOLERANCE:
        result["status"] = "py_newer"
        result["stale"] = "ipynb"
        result["recommendation"] = "Run sync direction=py2nb"
    elif ipynb_mtime - py_mtime > _MTIME_TOLERANCE:
        result["status"] = "ipynb_newer"
        result["stale"] = "py"
        result["recommendation"] = "Run sync direction=nb2py (human edited .ipynb)"
    else:
        result["status"] = "synced"
        result["recommendation"] = "Files are in sync"

    # Check if ipynb has execution outputs
    if ipynb_path.exists():
        try:
            nb_data = json.loads(ipynb_path.read_text(encoding="utf-8"))
            code_cells = [c for c in nb_data.get("cells", []) if c.get("cell_type") == "code"]
            result["executed"] = any(
                c.get("outputs") or c.get("execution_count")
                for c in code_cells
            )
            result["cell_count"] = len(nb_data.get("cells", []))
            result["code_cell_count"] = len(code_cells)
        except Exception:
            pass

    return result


# ---------------------------------------------------------------------------
# Lab manifest (symlink workspace for Jupyter Lab)
# ---------------------------------------------------------------------------

def nb_read(py_path: Path) -> dict[str, Any]:
    """Read a percent-format notebook and return content + metadata.

    Returns the ``.py`` source content alongside structured metadata
    (sections, imports, sync state) in a single call.
    """
    if not py_path.exists():
        return {"error": f"Notebook not found: {py_path}"}

    content = py_path.read_text(encoding="utf-8")
    result: dict[str, Any] = {
        "path": str(py_path),
        "name": py_path.stem,
        "content": content,
        "lines": content.count("\n") + 1,
    }

    # Add sync state
    result["sync"] = nb_diff(py_path)

    # Add structural analysis if available
    try:
        from pipeio.notebook.analyze import analyze_notebook
        analysis = analyze_notebook(py_path)
        result["sections"] = analysis.get("sections", [])
        result["imports"] = analysis.get("imports", [])
        result["run_card"] = analysis.get("run_card", [])
        result["cogpy_functions"] = analysis.get("cogpy_functions", [])
        result["pending_modules"] = analysis.get("pending_modules", [])
    except Exception:
        pass

    return result


def nb_audit(root: Path, registered_only: bool = True) -> list[dict[str, Any]]:
    """Audit all notebooks: staleness, config completeness, mod coverage.

    Returns a list of per-notebook audit records with quality flags.

    Parameters
    ----------
    registered_only : bool
        If True (default), only audit flows that are in the pipeline registry.
        Skips _template/ and other non-registered directories.
    """
    from pipeio.notebook.config import NotebookConfig

    # Build set of registered flow roots for filtering
    registered_roots: set[str] | None = None
    if registered_only:
        registered_roots = set()
        try:
            from pipeio.registry import PipelineRegistry
            for reg_candidate in (
                root / ".projio" / "pipeio" / "registry.yml",
                root / ".pipeio" / "registry.yml",
            ):
                if reg_candidate.exists():
                    registry = PipelineRegistry.from_yaml(reg_candidate)
                    for entry in registry.list_flows():
                        code_path = Path(entry.code_path)
                        if not code_path.is_absolute():
                            code_path = root / code_path
                        registered_roots.add(str(code_path.resolve()))
                    break
        except Exception:
            registered_roots = None  # Fall back to no filtering

    records: list[dict[str, Any]] = []

    for flow_root, cfg in find_notebook_configs(root):
        # Skip non-registered flows
        if registered_roots is not None:
            if str(flow_root.resolve()) not in registered_roots:
                continue
        # Collect mod names from the flow (if rule_list is available)
        flow_mods: set[str] = set()
        try:
            from pipeio.rules import parse_rules
            smk = flow_root / "Snakefile"
            if smk.exists():
                for rule in parse_rules(smk):
                    if rule.get("mod"):
                        flow_mods.add(rule["mod"])
        except Exception:
            pass

        notebook_mods: set[str] = set()

        for entry in cfg.entries:
            py_path = flow_root / entry.path
            kernel = cfg.resolve_kernel(entry)
            record: dict[str, Any] = {
                "name": py_path.stem,
                "flow_root": str(flow_root),
                "status": entry.status,
                "kind": entry.kind,
                "mod": entry.mod,
                "kernel": kernel,
                "issues": [],
            }

            if entry.mod:
                notebook_mods.add(entry.mod)

            # Check file exists
            if not py_path.exists():
                record["issues"].append("py_missing")
            else:
                record["lines"] = py_path.read_text(encoding="utf-8").count("\n") + 1

            # Check pairing config
            if not entry.pair_ipynb:
                record["issues"].append("pair_ipynb_disabled")

            # Check sync state
            if py_path.exists():
                diff = nb_diff(py_path)
                record["sync_status"] = diff.get("status", "unknown")
                if diff.get("status") == "ipynb_newer":
                    record["issues"].append("ipynb_has_unsynced_edits")
                elif diff.get("status") == "py_newer":
                    record["issues"].append("ipynb_stale")
                elif diff.get("status") == "unpaired":
                    record["issues"].append("ipynb_missing")
                record["executed"] = diff.get("executed", False)

            # Check metadata completeness
            if not entry.description:
                record["issues"].append("no_description")
            if not entry.kind:
                record["issues"].append("no_kind")
            if not kernel:
                record["issues"].append("no_kernel")
            if not entry.mod:
                record["issues"].append("no_mod")

            # Status quality
            if entry.status == "draft" and py_path.exists():
                lines = py_path.read_text(encoding="utf-8").count("\n")
                if lines > 50:
                    record["issues"].append("draft_but_substantial")

            # Lifecycle mismatch checks
            is_exploratory = entry.kind in ("investigate", "explore")
            is_demo = entry.kind in ("demo", "validate")

            # Demo notebook should have publish_html enabled
            if is_demo and not entry.publish_html:
                record["issues"].append("demo_not_publishable")

            # Exploratory notebook still active but mod already has scripts
            if is_exploratory and entry.status == "active" and entry.mod:
                if entry.mod in flow_mods:
                    record["issues"].append("explore_absorbed_not_archived")

            # Demo notebook active but not promoted (has been executed)
            if is_demo and entry.status == "active" and record.get("executed"):
                record["issues"].append("demo_executed_not_promoted")

            # Promoted notebook without publish_html
            if entry.status == "promoted" and not entry.publish_html:
                record["issues"].append("promoted_not_publishable")

            # Archived notebook still has publish_html on
            if entry.status == "archived" and entry.publish_html:
                record["issues"].append("archived_still_publishable")

            record["issue_count"] = len(record["issues"])
            records.append(record)

        # Check for mods without notebooks
        uncovered = flow_mods - notebook_mods
        if uncovered:
            records.append({
                "name": "__flow_coverage__",
                "flow_root": str(flow_root),
                "uncovered_mods": sorted(uncovered),
                "issues": [f"mod_without_notebook:{m}" for m in sorted(uncovered)],
                "issue_count": len(uncovered),
            })

    return records


def nb_migrate(root: Path, *, dry_run: bool = True) -> list[dict[str, Any]]:
    """Migrate notebooks from legacy layouts to the ``.src/`` / ``.myst/`` layout.

    Moves ``.py`` files into ``notebooks/.src/`` and ``.md`` files into
    ``notebooks/.myst/``.  ``.ipynb`` files stay in ``notebooks/``.
    Updates ``notebook.yml`` path entries.

    Parameters
    ----------
    dry_run : bool
        If True (default), only report what would change.  Pass False to execute.
    """
    from pipeio.notebook.config import NotebookConfig

    actions: list[dict[str, Any]] = []

    for flow_root, cfg in find_notebook_configs(root):
        nb_dir = flow_root / "notebooks"
        cfg_path = nb_dir / "notebook.yml"
        cfg_modified = False

        for entry in cfg.entries:
            py_path = flow_root / entry.path

            # Already in .src/ layout — skip
            if ".src" in entry.path:
                continue

            name = py_path.stem
            src_dir = nb_dir / ".src"
            myst_dir = nb_dir / ".myst"
            new_py = src_dir / f"{name}.py"
            new_md = myst_dir / f"{name}.md"
            new_ipynb = nb_dir / f"{name}.ipynb"

            action: dict[str, Any] = {
                "name": name,
                "flow_root": str(flow_root),
                "moves": [],
            }

            # Find the current .py file (could be flat or in subdir)
            if py_path.exists():
                action["moves"].append({"from": str(py_path), "to": str(new_py)})
            elif (nb_dir / name / f"{name}.py").exists():
                py_path = nb_dir / name / f"{name}.py"
                action["moves"].append({"from": str(py_path), "to": str(new_py)})

            # Find paired .md
            old_md = py_path.with_suffix(".md") if py_path.exists() else None
            if old_md and old_md.exists():
                action["moves"].append({"from": str(old_md), "to": str(new_md)})

            # Find paired .ipynb — move to flat notebooks/ if in subdir
            old_ipynb = py_path.with_suffix(".ipynb") if py_path.exists() else None
            if old_ipynb and old_ipynb.exists() and old_ipynb.parent != nb_dir:
                action["moves"].append({"from": str(old_ipynb), "to": str(new_ipynb)})

            if not action["moves"]:
                continue

            # Update notebook.yml path
            new_rel = f"notebooks/.src/{name}.py"
            action["path_update"] = {"from": entry.path, "to": new_rel}

            if not dry_run:
                src_dir.mkdir(parents=True, exist_ok=True)
                myst_dir.mkdir(parents=True, exist_ok=True)
                for move in action["moves"]:
                    src = Path(move["from"])
                    dst = Path(move["to"])
                    if src.exists():
                        shutil.move(str(src), str(dst))

                # Clean up empty subdirectory
                old_subdir = nb_dir / name
                if old_subdir.is_dir() and not any(old_subdir.iterdir()):
                    old_subdir.rmdir()

                entry.path = new_rel
                cfg_modified = True

            actions.append(action)

        if cfg_modified:
            cfg.to_yaml(cfg_path)

    return actions


def nb_lab(
    root: Path,
    *,
    flow: str | None = None,
    lab_dir: Path | None = None,
    sync: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Build a symlink manifest of active .ipynb notebooks and return its state.

    Creates ``<lab_dir>/<flow>/<name>.ipynb`` symlinks pointing back to
    the real notebook files.  Stale symlinks (pointing to removed notebooks)
    are cleaned up automatically.

    Parameters
    ----------
    root : Path
        Project root.
    flow : str | None
        Filter to a specific flow.
    lab_dir : Path | None
        Manifest directory (default: ``<root>/.projio/pipeio/lab``).
    sync : bool
        If True, sync py→ipynb before linking so notebooks are fresh
        (default False).
    python_bin : str | None
        Python binary where jupytext is installed (for sync).
    """
    if lab_dir is None:
        lab_dir = root / ".projio" / "pipeio" / "lab"
    lab_dir.mkdir(parents=True, exist_ok=True)

    from pipeio.notebook.config import NotebookConfig

    linked: list[dict[str, str]] = []
    synced: list[str] = []

    # Build flow_root → flow_name lookup from registry
    flow_lookup: dict[str, str] = {}
    try:
        from pipeio.registry import PipelineRegistry
        for reg_candidate in (
            root / ".projio" / "pipeio" / "registry.yml",
            root / ".pipeio" / "registry.yml",
        ):
            if reg_candidate.exists():
                registry = PipelineRegistry.from_yaml(reg_candidate)
                for entry in registry.list_flows():
                    code_path = Path(entry.code_path)
                    if not code_path.is_absolute():
                        code_path = root / code_path
                    flow_lookup[str(code_path.resolve())] = entry.name
                break
    except Exception:
        pass

    for flow_root, cfg in find_notebook_configs(root):
        # Look up flow name from registry
        resolved_root = str(flow_root.resolve())
        if resolved_root in flow_lookup:
            entry_flow = flow_lookup[resolved_root]
        else:
            # Fallback: derive from directory name
            entry_flow = flow_root.name

        if flow and entry_flow != flow:
            continue

        for entry in cfg.entries:
            # Only link active notebooks with ipynb pairing
            if entry.status not in ("active", "draft") or not entry.pair_ipynb:
                continue

            py_path = flow_root / entry.path
            ipynb_path, _ = _nb_output_paths(py_path)
            kernel = cfg.resolve_kernel(entry)

            # Optionally sync first (with kernel embedded)
            if sync and py_path.exists():
                result = nb_sync_one(
                    py_path, direction="py2nb", formats=["ipynb"],
                    force=False, kernel=kernel, python_bin=python_bin,
                )
                if result.get("synced"):
                    synced.append(str(ipynb_path))

            if not ipynb_path.exists():
                continue

            # Create symlink: lab_dir/<flow>[/<workspace>]/<name>.ipynb
            kind = getattr(entry, "kind", "") or ""
            if kind in ("investigate", "explore"):
                link_dir = lab_dir / entry_flow / "explore"
            elif kind in ("demo", "validate"):
                link_dir = lab_dir / entry_flow / "demo"
            else:
                link_dir = lab_dir / entry_flow
            link_dir.mkdir(parents=True, exist_ok=True)
            link_path = link_dir / ipynb_path.name

            # Use absolute path — lab dir is gitignored, no portability concern
            abs_target = ipynb_path.resolve()

            if link_path.is_symlink() or link_path.exists():
                link_path.unlink()
            link_path.symlink_to(abs_target)

            item: dict[str, str] = {
                "name": py_path.stem,
                "flow": entry_flow,
                "link": str(link_path.relative_to(lab_dir)),
                "target": str(ipynb_path),
            }
            if kernel:
                item["kernel"] = kernel
            linked.append(item)

    # Clean stale symlinks
    stale: list[str] = []
    for link in lab_dir.rglob("*.ipynb"):
        if link.is_symlink() and not link.resolve().exists():
            stale.append(str(link.relative_to(lab_dir)))
            link.unlink()
    # Remove empty directories
    for d in sorted(lab_dir.rglob("*"), reverse=True):
        if d.is_dir() and not any(d.iterdir()):
            d.rmdir()

    return {
        "lab_dir": str(lab_dir),
        "linked": linked,
        "synced": synced,
        "stale_cleaned": stale,
        "count": len(linked),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_jupytext(python_bin: str | None = None) -> None:
    """Check that jupytext is available.

    When *python_bin* is given, probe via subprocess (the tool may live in a
    different Python environment).  Otherwise fall back to an in-process import.
    """
    if python_bin:
        import subprocess as _sp
        try:
            _sp.run(
                [python_bin, "-m", "jupytext", "--version"],
                capture_output=True, check=True, timeout=15,
            )
            return
        except (FileNotFoundError, _sp.CalledProcessError, _sp.TimeoutExpired):
            raise ImportError(
                f"jupytext not found via {python_bin}. "
                "Install with: pip install pipeio[notebook]"
            )
    try:
        import jupytext  # noqa: F401
    except ImportError:
        raise ImportError(
            "Notebook operations require jupytext. "
            "Install with: pip install pipeio[notebook]"
        )


def _require_nbconvert() -> None:
    try:
        import nbconvert  # noqa: F401
    except ImportError:
        raise ImportError(
            "Notebook execution requires nbconvert. "
            "Install with: pip install pipeio[notebook]"
        )


def _jupytext(source: Path, *args: str, python_bin: str | None = None) -> None:
    """Run jupytext on *source*.

    When *python_bin* is given, invoke as ``python_bin -m jupytext`` so the
    tool can live in a different environment from the MCP server.
    """
    if python_bin:
        cmd = [python_bin, "-m", "jupytext", str(source), *args]
    else:
        cmd = ["jupytext", str(source), *args]
    subprocess.run(cmd, check=True)


def _nbconvert_exec(nb_path: Path) -> None:
    subprocess.run(
        [
            "jupyter", "nbconvert",
            "--to", "notebook",
            "--execute",
            "--inplace",
            str(nb_path),
        ],
        check=True,
    )


def _nbconvert_html(nb_path: Path, output: Path) -> None:
    subprocess.run(
        [
            "jupyter", "nbconvert",
            "--to", "html",
            str(nb_path),
            "--output", str(output),
        ],
        check=True,
    )
