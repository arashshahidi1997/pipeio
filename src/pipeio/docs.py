"""Pipeline docs collection: write-local, publish-to-site.

Flow authors write docs next to their pipeline code::

    code/pipelines/preproc/denoise/
      docs/
        index.md
        mod-smoothing.md
      notebooks/
        notebook.yml
        analysis.py

``docs_collect`` assembles them into ``docs/pipelines/<pipe>/<flow>/``
for MkDocs, and ``docs_nav`` emits a YAML nav fragment.
"""

from __future__ import annotations

import shutil
import subprocess
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


def docs_collect(root: Path) -> list[str]:
    """Collect flow-local docs and notebook outputs into ``docs/pipelines/``.

    For each flow in the registry:

    1. Copy ``<flow_dir>/docs/*`` to ``docs/pipelines/<pipe>/<flow>/``
    2. Publish notebooks to ``docs/pipelines/<pipe>/<flow>/notebooks/``
       (HTML by default, or MyST if configured)

    Returns list of collected/published file paths.
    """
    from pipeio.registry import PipelineRegistry

    registry_path = _find_registry(root)
    if registry_path is None:
        return []

    registry = PipelineRegistry.from_yaml(registry_path)
    docs_base = root / "docs" / "pipelines"
    collected: list[str] = []

    for entry in registry.list_flows():
        flow_dir = Path(entry.code_path)
        if not flow_dir.is_absolute():
            flow_dir = root / flow_dir
        if not flow_dir.is_dir():
            continue

        target = docs_base / entry.pipe / entry.name

        # --- 1. Collect hand-written docs ---
        flow_docs = flow_dir / "docs"
        if flow_docs.is_dir():
            for src_file in sorted(flow_docs.rglob("*")):
                if not src_file.is_file():
                    continue
                rel = src_file.relative_to(flow_docs)
                dst = target / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst)
                collected.append(str(dst))

        # --- 2. Publish notebooks ---
        nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
        if not nb_cfg_path.exists():
            continue

        from pipeio.notebook.config import NotebookConfig

        try:
            nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
        except Exception:
            continue

        nb_target = target / "notebooks"
        fmt = nb_cfg.publish.format  # "html" or "myst"

        for nb_entry in nb_cfg.entries:
            py_path = flow_dir / nb_entry.path
            name = py_path.stem

            if nb_entry.publish_html or (fmt == "html" and (nb_entry.publish_html or nb_entry.publish_myst)):
                ipynb = py_path.with_suffix(".ipynb")
                if ipynb.exists():
                    nb_target.mkdir(parents=True, exist_ok=True)
                    out = nb_target / f"{name}.html"
                    _nbconvert_html(ipynb, out)
                    collected.append(str(out))

            if nb_entry.publish_myst and fmt == "myst":
                myst = py_path.with_suffix(".md")
                if myst.exists():
                    nb_target.mkdir(parents=True, exist_ok=True)
                    out = nb_target / f"{name}.md"
                    shutil.copy2(myst, out)
                    collected.append(str(out))

    return collected


def docs_nav(root: Path) -> str:
    """Generate a MkDocs nav YAML fragment for ``docs/pipelines/``.

    Returns a YAML string suitable for pasting into ``mkdocs.yml``::

        - Pipelines:
          - preproc:
            - denoise:
              - Overview: pipelines/preproc/denoise/index.md
              - Notebooks:
                - Analysis: pipelines/preproc/denoise/notebooks/analysis.html
    """
    docs_root = root / "docs"
    docs_base = docs_root / "pipelines"
    if not docs_base.exists():
        return "# No docs/pipelines/ directory found.\n"

    nav: dict[str, Any] = {}

    for pipe_dir in sorted(d for d in docs_base.iterdir() if d.is_dir()):
        pipe_nav: list[dict[str, Any]] = []

        for flow_dir in sorted(d for d in pipe_dir.iterdir() if d.is_dir()):
            flow_entries: list[dict[str, Any]] = []

            # index.md first
            idx = flow_dir / "index.md"
            if idx.exists():
                flow_entries.append(
                    {"Overview": str(idx.relative_to(docs_root))}
                )

            # other .md files (excluding index)
            for md in sorted(flow_dir.glob("*.md")):
                if md.name == "index.md":
                    continue
                title = md.stem.replace("-", " ").replace("_", " ").title()
                flow_entries.append(
                    {title: str(md.relative_to(docs_root))}
                )

            # notebooks subdirectory
            nb_dir = flow_dir / "notebooks"
            if nb_dir.is_dir():
                nb_entries: list[dict[str, str]] = []
                for f in sorted(nb_dir.iterdir()):
                    if f.suffix in (".html", ".md") and f.is_file():
                        title = f.stem.replace("-", " ").replace("_", " ").title()
                        nb_entries.append(
                            {title: str(f.relative_to(docs_root))}
                        )
                if nb_entries:
                    flow_entries.append({"Notebooks": nb_entries})

            if flow_entries:
                pipe_nav.append({flow_dir.name: flow_entries})

        if pipe_nav:
            nav[pipe_dir.name] = pipe_nav

    if not nav:
        return "# docs/pipelines/ exists but contains no docs.\n"

    fragment = [{"Pipelines": nav}]
    return yaml.dump(fragment, sort_keys=False, default_flow_style=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nbconvert_html(nb_path: Path, output: Path) -> None:
    """Convert a notebook to HTML."""
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "jupyter", "nbconvert",
            "--to", "html",
            str(nb_path),
            "--output", str(output.name),
            "--output-dir", str(output.parent),
        ],
        check=True,
    )
