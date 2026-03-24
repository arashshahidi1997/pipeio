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
# Helpers
# ---------------------------------------------------------------------------

def _require_jupytext() -> None:
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


def _jupytext(source: Path, *args: str) -> None:
    subprocess.run(["jupytext", str(source), *args], check=True)


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
