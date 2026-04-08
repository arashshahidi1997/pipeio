"""Pipeline docs collection: write-local, publish-to-site.

Flow authors write docs next to their pipeline code::

    code/pipelines/denoise/
      docs/
        index.md
        mod-smoothing.md
      notebooks/
        notebook.yml
        analysis.py

``docs_collect`` assembles them into ``docs/pipelines/<flow>/``
for MkDocs, and ``docs_nav`` emits a YAML nav fragment.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


class PublishConfig(BaseModel):
    """Flow-level publish config (``publish.yml``)."""

    dag: bool = False
    report: bool = False
    report_archive: bool = False
    scripts: bool = False

    @classmethod
    def from_yaml(cls, path: Path) -> PublishConfig:
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}
        return cls(**raw)


def _find_registry(root: Path) -> Path | None:
    """Locate the pipeline registry, checking .projio/pipeio/ first."""
    from pipeio.registry import find_registry
    return find_registry(root)


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

    # Generate top-level index.md for pipelines section
    pipelines_index = docs_base / "index.md"
    if not pipelines_index.exists():
        docs_base.mkdir(parents=True, exist_ok=True)
        flow_names = [f.name for f in registry.list_flows()]
        lines = ["# Pipelines", ""]
        if flow_names:
            for name in flow_names:
                lines.append(f"- [{name}]({name}/)")
        else:
            lines.append("No flows registered yet.")
        lines.append("")
        pipelines_index.write_text("\n".join(lines), encoding="utf-8")
        collected.append(str(pipelines_index))

    for entry in registry.list_flows():
        flow_dir = Path(entry.code_path)
        if not flow_dir.is_absolute():
            flow_dir = root / flow_dir
        if not flow_dir.is_dir():
            continue

        target = docs_base / entry.name

        # --- 1. Collect hand-written docs ---
        # Faceted mod docs (docs/{mod}/theory.md) → mods/{mod}/theory.md
        # Flow-level docs (docs/index.md) → index.md (preserved as-is)
        flow_docs = flow_dir / "docs"
        if flow_docs.is_dir():
            # Detect mod subdirectories (contain theory.md or spec.md)
            mod_dirs = {
                d.name for d in flow_docs.iterdir()
                if d.is_dir() and any(
                    (d / f).exists() for f in ("theory.md", "spec.md", "delta.md")
                )
            }
            for src_file in sorted(flow_docs.rglob("*")):
                if not src_file.is_file():
                    continue
                rel = src_file.relative_to(flow_docs)
                parts = rel.parts
                # Route mod facet dirs into mods/ subdirectory
                if parts and parts[0] in mod_dirs:
                    dst = target / "mods" / rel
                # overview.md at flow root → copy as index.md to avoid
                # duplicate "Overview" nav entries
                elif rel == Path("overview.md"):
                    dst = target / "index.md"
                else:
                    dst = target / rel
                if _is_stale(src_file, dst):
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_file, dst)
                collected.append(str(dst))

        # --- 2. Publish notebooks ---
        nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
        if nb_cfg_path.exists():
            from pipeio.notebook.config import NotebookConfig

            try:
                nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
                nb_target = target / "notebooks"
                fmt = nb_cfg.publish.format  # "html" or "myst"

                for nb_entry in nb_cfg.entries:
                    py_path = flow_dir / nb_entry.path
                    name = py_path.stem

                    # Use _nb_output_paths for workspace-aware path resolution
                    from pipeio.notebook.lifecycle import _nb_output_paths
                    ipynb_path, myst_path = _nb_output_paths(py_path)

                    # Explore notebooks are never published by default
                    kind = getattr(nb_entry, "kind", "") or ""
                    if kind in ("investigate", "explore") and not nb_entry.publish_html and not nb_entry.publish_myst:
                        continue

                    if nb_entry.publish_html or (fmt == "html" and (nb_entry.publish_html or nb_entry.publish_myst)):
                        if ipynb_path.exists():
                            nb_target.mkdir(parents=True, exist_ok=True)
                            out = nb_target / f"{name}.html"
                            if _is_stale(ipynb_path, out):
                                _nbconvert_html(ipynb_path, out)
                            collected.append(str(out))

                    if nb_entry.publish_myst and fmt == "myst":
                        if myst_path.exists():
                            nb_target.mkdir(parents=True, exist_ok=True)
                            out = nb_target / f"{name}.md"
                            if _is_stale(myst_path, out):
                                shutil.copy2(myst_path, out)
                            collected.append(str(out))
            except Exception:
                pass

        # Generate notebooks/index.md if notebooks were published
        nb_target = target / "notebooks"
        if nb_target.is_dir():
            nb_files = sorted(
                f for f in nb_target.iterdir()
                if f.is_file() and f.suffix in (".html", ".md") and f.name != "index.md"
            )
            if nb_files:
                nb_index = nb_target / "index.md"
                lines = [f"# Notebooks — {entry.name}", ""]
                for f in nb_files:
                    title = f.stem.replace("-", " ").replace("_", " ").title()
                    lines.append(f"- [{title}]({f.name})")
                lines.append("")
                nb_index.write_text("\n".join(lines), encoding="utf-8")
                collected.append(str(nb_index))

        # --- 3. publish.yml artifacts (DAG, report, scripts) ---
        pub_cfg_path = flow_dir / "publish.yml"
        if pub_cfg_path.exists():
            try:
                pub_cfg = PublishConfig.from_yaml(pub_cfg_path)
            except Exception:
                pub_cfg = PublishConfig()

            # DAG: copy dag.svg if present
            if pub_cfg.dag:
                dag_src = flow_dir / "dag.svg"
                if dag_src.exists():
                    dst = target / "dag.svg"
                    if _is_stale(dag_src, dst):
                        target.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(dag_src, dst)
                    collected.append(str(dst))

            # Report: link to report.html in derivatives (too heavy to copy)
            if pub_cfg.report:
                report_candidates = [
                    flow_dir / "report.html",
                    root / "derivatives" / entry.name / "report.html",
                ]
                report_src = next((r for r in report_candidates if r.exists()), None)
                if report_src is not None:
                    target.mkdir(parents=True, exist_ok=True)
                    dst = target / "report.md"
                    rel_report = report_src.relative_to(root)
                    dst.write_text(
                        f"# Report — {entry.name}\n\n"
                        f"[Open full Snakemake report](/{rel_report})\n\n"
                        f"Generated from {len(list(report_src.parent.glob('*.html')))} HTML file(s) "
                        f"in `{rel_report.parent}/`.\n",
                        encoding="utf-8",
                    )
                    collected.append(str(dst))

            # Scripts: generate script index with git links
            if pub_cfg.scripts:
                scripts_dir = flow_dir / "scripts"
                if scripts_dir.is_dir():
                    script_files = sorted(scripts_dir.glob("*.py"))
                    if script_files:
                        target.mkdir(parents=True, exist_ok=True)
                        dst = target / "scripts.md"
                        dst.write_text(
                            _generate_scripts_index(
                                entry.name, script_files, flow_dir, root
                            ),
                            encoding="utf-8",
                        )
                        collected.append(str(dst))

        # --- 4. Generate flow index.md if missing ---
        flow_index = target / "index.md"
        if not flow_index.exists():
            target.mkdir(parents=True, exist_ok=True)
            flow_index.write_text(
                f"# {entry.name}\n\n"
                f"Pipeline flow documentation.\n",
                encoding="utf-8",
            )
            collected.append(str(flow_index))

        # --- 5. Auto-generate DAG SVG if not already collected ---
        dag_dst = target / "dag.svg"
        if str(dag_dst) not in collected:
            snakefile = flow_dir / "Snakefile"
            if snakefile.exists():
                try:
                    dag_svg = _generate_dag_svg(root, entry, snakefile)
                    if dag_svg:
                        target.mkdir(parents=True, exist_ok=True)
                        dag_dst.write_text(dag_svg, encoding="utf-8")
                        collected.append(str(dag_dst))
                except Exception:
                    pass  # non-fatal: DAG generation is best-effort

    # --- 6. Write docs/pipelines/mkdocs.yml for monorepo plugin ---
    if collected:
        nav_yaml = docs_nav(root, write=True)
        sub_mkdocs = docs_base / "mkdocs.yml"
        if sub_mkdocs.exists():
            collected.append(str(sub_mkdocs))

    return collected


def _is_stale(src: Path, dst: Path) -> bool:
    """Return True if *dst* is missing or older than *src*."""
    if not dst.exists():
        return True
    return src.stat().st_mtime > dst.stat().st_mtime


def _generate_dag_svg(
    root: Path, entry: Any, snakefile: Path,
) -> str | None:
    """Generate a rulegraph SVG via snakemake + graphviz. Returns SVG string or None."""
    import shutil

    if not shutil.which("dot"):
        return None

    flow_dir = snakefile.parent

    # Resolve snakemake command
    smk_cmd = _resolve_snakemake_for_docs()

    cmd = [
        *smk_cmd,
        "--snakefile", str(snakefile),
        "--directory", str(flow_dir),
        "--rulegraph",
    ]

    result = subprocess.run(
        cmd, capture_output=True, text=True, cwd=str(root), timeout=60, check=False,
    )
    if result.returncode != 0:
        return None

    dot_output = result.stdout
    if not dot_output.strip():
        return None

    svg_result = subprocess.run(
        ["dot", "-Tsvg"], input=dot_output, capture_output=True,
        text=True, timeout=30, check=False,
    )
    if svg_result.returncode != 0:
        return None

    return svg_result.stdout


def _resolve_snakemake_for_docs() -> list[str]:
    """Find snakemake for DAG generation during docs_collect."""
    import shutil

    binary = shutil.which("snakemake")
    if binary:
        return [binary]

    # Try common conda env names
    import re

    for env in ("cogpy", "snakemake"):
        for base in ("/storage/share/python/environments/Anaconda3",):
            for rel in ("condabin/conda", "bin/conda"):
                conda = Path(base) / rel
                if conda.is_file():
                    return [str(conda), "run", "-n", env, "snakemake"]

    return ["snakemake"]


def docs_nav(root: Path, *, write: bool = True) -> str:
    """Generate nav for ``docs/pipelines/`` and optionally write the monorepo sub-mkdocs.yml.

    When ``write=True`` (default), writes ``docs/pipelines/mkdocs.yml`` — a
    standalone MkDocs config consumed by the ``mkdocs-monorepo-plugin`` via
    ``!include ./docs/pipelines/mkdocs.yml`` in the root ``mkdocs.yml``.

    Always returns the generated YAML string (the nav fragment for legacy use).
    """
    docs_root = root / "docs"
    docs_base = docs_root / "pipelines"
    if not docs_base.exists():
        return "# No docs/pipelines/ directory found.\n"

    flow_navs: list[dict[str, Any]] = []

    for flow_dir in sorted(d for d in docs_base.iterdir() if d.is_dir()):
        flow_entries: list[dict[str, Any]] = []

        # index.md first
        idx = flow_dir / "index.md"
        if idx.exists():
            flow_entries.append(
                {"Overview": str(idx.relative_to(docs_base))}
            )

        # other .md files (excluding index and overview which is used as index)
        for md in sorted(flow_dir.glob("*.md")):
            if md.name in ("index.md", "overview.md"):
                continue
            title = md.stem.replace("-", " ").replace("_", " ").title()
            flow_entries.append(
                {title: str(md.relative_to(docs_base))}
            )

        # DAG SVG
        dag = flow_dir / "dag.svg"
        if dag.exists():
            flow_entries.append(
                {"DAG": str(dag.relative_to(docs_base))}
            )

        # notebooks subdirectory
        nb_dir = flow_dir / "notebooks"
        if nb_dir.is_dir():
            nb_entries: list[dict[str, Any]] = []
            # Use index.md as landing page if present
            nb_idx = nb_dir / "index.md"
            if nb_idx.exists():
                nb_entries.append(
                    {"Overview": str(nb_idx.relative_to(docs_base))}
                )
            for f in sorted(nb_dir.iterdir()):
                if f.name == "index.md":
                    continue
                if f.suffix in (".html", ".md") and f.is_file():
                    title = f.stem.replace("-", " ").replace("_", " ").title()
                    nb_entries.append(
                        {title: str(f.relative_to(docs_base))}
                    )
            if nb_entries:
                flow_entries.append({"Notebooks": nb_entries})

        # mods subdirectory (theory.md, spec.md per mod)
        mods_dir = flow_dir / "mods"
        if mods_dir.is_dir():
            mod_nav_entries: list[dict[str, Any]] = []
            for mod_dir in sorted(d for d in mods_dir.iterdir() if d.is_dir()):
                mod_pages: list[dict[str, str]] = []
                for md in sorted(mod_dir.glob("*.md")):
                    title = md.stem.replace("-", " ").replace("_", " ").title()
                    mod_pages.append(
                        {title: str(md.relative_to(docs_base))}
                    )
                if mod_pages:
                    mod_nav_entries.append({mod_dir.name: mod_pages})
            if mod_nav_entries:
                flow_entries.append({"Modules": mod_nav_entries})

        if flow_entries:
            flow_navs.append({flow_dir.name: flow_entries})

    if not flow_navs:
        return "# docs/pipelines/ exists but contains no docs.\n"

    # Build the sub-mkdocs.yml for monorepo plugin
    sub_config = {
        "site_name": "pipelines",
        "docs_dir": ".",
        "nav": flow_navs,
    }
    sub_yaml = yaml.dump(sub_config, sort_keys=False, default_flow_style=False)

    if write:
        sub_mkdocs = docs_base / "mkdocs.yml"
        sub_mkdocs.write_text(sub_yaml, encoding="utf-8")

    # Also return the legacy fragment format
    fragment = [{"Pipelines": flow_navs}]
    return yaml.dump(fragment, sort_keys=False, default_flow_style=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_scripts_index(
    flow_name: str,
    script_files: list[Path],
    flow_dir: Path,
    root: Path,
) -> str:
    """Generate a markdown script index."""
    lines = [
        f"# Scripts — {flow_name}",
        "",
        "| Script | Description |",
        "|--------|-------------|",
    ]
    for sf in script_files:
        rel = sf.relative_to(root)
        # Extract docstring (first line of triple-quoted string)
        desc = ""
        try:
            text = sf.read_text(encoding="utf-8")
            if text.startswith('"""') or text.startswith("'''"):
                end = text.find('"""', 3) if text.startswith('"""') else text.find("'''", 3)
                if end > 0:
                    desc = text[3:end].strip().split("\n")[0]
        except Exception:
            pass
        # Use code-formatted text instead of links (scripts are outside docs_dir)
        lines.append(f"| `{sf.name}` | {desc} |")
    lines.append("")
    return "\n".join(lines)


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
