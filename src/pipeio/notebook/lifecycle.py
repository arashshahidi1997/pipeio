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
            ipynb_path = py_path.with_suffix(".ipynb") if entry.pair_ipynb else None
            myst_path = py_path.with_suffix(".md") if entry.pair_myst else None

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

            if entry.pair_ipynb:
                ipynb_path = py_path.with_suffix(".ipynb")
                if force or not ipynb_path.exists():
                    _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path))
                    created.append(str(ipynb_path))

            if entry.pair_myst:
                myst_path = py_path.with_suffix(".md")
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

            if entry.pair_ipynb:
                ipynb_path = py_path.with_suffix(".ipynb")
                if ipynb_path.exists() and ipynb_path.stat().st_mtime < py_mtime:
                    _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path))
                    updated.append(str(ipynb_path))

            if entry.pair_myst:
                myst_path = py_path.with_suffix(".md")
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
            ipynb_path = py_path.with_suffix(".ipynb")
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

            if entry.publish_html:
                _require_nbconvert()
                ipynb_path = py_path.with_suffix(".ipynb")
                if ipynb_path.exists():
                    out = docs_dir / f"{cfg.publish.prefix}{name}.html"
                    _nbconvert_html(ipynb_path, out)
                    published.append(str(out))

            if entry.publish_myst:
                _require_jupytext()
                myst_path = py_path.with_suffix(".md")
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
        return _sync_py2nb(py_path, formats=formats, force=force, python_bin=python_bin)
    else:
        return {"error": f"Unknown direction: {direction!r}. Use 'py2nb' or 'nb2py'."}


def _sync_py2nb(
    py_path: Path,
    *,
    formats: list[str],
    force: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Sync .py → .ipynb / .myst."""
    if not py_path.exists():
        return {"error": f"Source not found: {py_path}"}

    py_mtime = py_path.stat().st_mtime
    generated: list[str] = []

    if "ipynb" in formats:
        ipynb_path = py_path.with_suffix(".ipynb")
        if force or not ipynb_path.exists() or ipynb_path.stat().st_mtime < py_mtime:
            _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path), python_bin=python_bin)
            generated.append(str(ipynb_path))

    if "myst" in formats:
        myst_path = py_path.with_suffix(".md")
        if force or not myst_path.exists() or myst_path.stat().st_mtime < py_mtime:
            _jupytext(py_path, "--to", "myst", "--output", str(myst_path), python_bin=python_bin)
            generated.append(str(myst_path))

    return {
        "synced": bool(generated),
        "skipped": not generated,
        "direction": "py2nb",
        "source": str(py_path),
        "generated": generated,
    }


def _sync_nb2py(
    py_path: Path,
    *,
    force: bool = False,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Sync .ipynb → .py (reverse sync for human edits)."""
    ipynb_path = py_path.with_suffix(".ipynb")
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
    ipynb_path = py_path.with_suffix(".ipynb")

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

    if py_mtime > ipynb_mtime:
        result["status"] = "py_newer"
        result["stale"] = "ipynb"
        result["recommendation"] = "Run sync direction=py2nb"
    elif ipynb_mtime > py_mtime:
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

def nb_lab(
    root: Path,
    *,
    pipe: str | None = None,
    flow: str | None = None,
    lab_dir: Path | None = None,
    sync: bool = True,
    python_bin: str | None = None,
) -> dict[str, Any]:
    """Build a symlink manifest of active .ipynb notebooks and return its state.

    Creates ``<lab_dir>/<pipe>/<flow>/<name>.ipynb`` symlinks pointing back to
    the real notebook files.  Stale symlinks (pointing to removed notebooks)
    are cleaned up automatically.

    Parameters
    ----------
    root : Path
        Project root.
    pipe : str | None
        Filter to a specific pipeline.
    flow : str | None
        Filter to a specific flow.
    lab_dir : Path | None
        Manifest directory (default: ``<root>/.projio/pipeio/lab``).
    sync : bool
        If True (default), sync py→ipynb before linking so notebooks are fresh.
    python_bin : str | None
        Python binary where jupytext is installed (for sync).
    """
    if lab_dir is None:
        lab_dir = root / ".projio" / "pipeio" / "lab"
    lab_dir.mkdir(parents=True, exist_ok=True)

    from pipeio.notebook.config import NotebookConfig

    linked: list[dict[str, str]] = []
    synced: list[str] = []

    for flow_root, cfg in find_notebook_configs(root):
        # Derive pipe/flow from path: <root>/pipe/<pipe>/<flow>/
        try:
            rel = flow_root.relative_to(root)
            parts = rel.parts  # e.g. ("pipe", "s01-preproc", "flow1")
        except ValueError:
            continue

        # Registry layout: pipe/<pipe_name>/<flow_name>/ or pipelines/<pipe>/<flow>/
        entry_pipe = parts[1] if len(parts) >= 3 else parts[0] if parts else "unknown"
        entry_flow = parts[2] if len(parts) >= 3 else parts[1] if len(parts) >= 2 else "unknown"

        if pipe and entry_pipe != pipe:
            continue
        if flow and entry_flow != flow:
            continue

        for entry in cfg.entries:
            # Only link active notebooks with ipynb pairing
            if entry.status not in ("active", "draft") or not entry.pair_ipynb:
                continue

            py_path = flow_root / entry.path
            ipynb_path = py_path.with_suffix(".ipynb")

            # Optionally sync first
            if sync and py_path.exists():
                result = nb_sync_one(
                    py_path, direction="py2nb", formats=["ipynb"],
                    force=False, python_bin=python_bin,
                )
                if result.get("synced"):
                    synced.append(str(ipynb_path))

            if not ipynb_path.exists():
                continue

            # Create symlink: lab_dir/<pipe>/<flow>/<name>.ipynb
            link_dir = lab_dir / entry_pipe / entry_flow
            link_dir.mkdir(parents=True, exist_ok=True)
            link_path = link_dir / ipynb_path.name

            # Compute relative target from link location to real file
            target = Path.cwd() if not ipynb_path.is_absolute() else ipynb_path
            target = ipynb_path
            try:
                rel_target = Path(*(['..'] * len(link_path.parent.relative_to(lab_dir).parts)),
                                  ipynb_path.relative_to(root))
            except ValueError:
                rel_target = ipynb_path  # absolute fallback

            if link_path.is_symlink():
                link_path.unlink()
            link_path.symlink_to(rel_target)

            linked.append({
                "name": py_path.stem,
                "pipe": entry_pipe,
                "flow": entry_flow,
                "link": str(link_path.relative_to(lab_dir)),
                "target": str(ipynb_path),
            })

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
