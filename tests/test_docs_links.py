"""docs_collect fixes relative links for the collected tree (issue 20261108-230959).

Collected flow docs move from ``code/pipelines/<flow>/docs/`` to
``docs/pipelines/<flow>/`` (mod facets to ``mods/``), so links written
against the source tree break.  The fixture reproduces each kind found in
pixecog's site and checks the result with a strict mkdocs build.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _project(root: Path) -> Path:
    pipes = root / "code" / "pipelines"
    # Flow "alpha": template index linking report/changelog (neither collected),
    # a mod facet, links to another flow's source docs, to site pages and to code.
    _write(pipes / "alpha" / "Snakefile", "")
    _write(pipes / "alpha" / "config.yml", "output_dir: derivatives/alpha\n")
    _write(pipes / "alpha" / "publish.yml", "report: true\nchangelog: true\n")
    _write(pipes / "alpha" / "docs" / "index.md", (
        "# alpha\n\n"
        "- Mod: [detect theory](detect/theory.md) and [spec](detect/spec.md#inputs)\n"
        "- See [Snakemake report](report.md) and [CHANGELOG](changelog.md)\n"
        "- Env: [pygfet](../../../envs/pygfet/index.md)\n"
        "- Meeting: [notes](../../../../docs/log/meeting/m1.md)\n"
        "- Web: [site](https://example.org) and [top](#alpha)\n\n"
        "```\n[not a link](report.md)\n```\n"
        "Inline `[code](report.md)` stays.\n"
    ))
    _write(pipes / "alpha" / "docs" / "detect" / "theory.md", (
        "# Theory\n\n"
        "Back to the [flow](../index.md#alpha); helper [utils/detect.py](../../../../utils/detect.py);\n"
        "upstream [beta detect](../../../beta/docs/detect/theory.md);\n"
        "![fig](fig.png)\n"
    ))
    _write(pipes / "alpha" / "docs" / "detect" / "spec.md", "# Spec\n\n## Inputs\n")
    (pipes / "alpha" / "docs" / "detect" / "fig.png").write_bytes(b"\x89PNG\r\n")
    # CHANGELOG at flow root links a meeting note (outside the flow).
    _write(pipes / "alpha" / "CHANGELOG.md", (
        "# Changelog\n\n- Decided in [meeting](../../../docs/log/meeting/m1.md)\n"
    ))
    # Flow "beta": changelog not published, so alpha's index link must go.
    _write(pipes / "beta" / "Snakefile", "")
    _write(pipes / "beta" / "config.yml", "output_dir: derivatives/beta\n")
    _write(pipes / "beta" / "docs" / "index.md", "# beta\n\nSee [CHANGELOG](changelog.md).\n")
    _write(pipes / "beta" / "docs" / "detect" / "theory.md", "# Beta theory\n")
    _write(pipes / "beta" / "CHANGELOG.md", "# Changelog\n")
    _write(pipes / "architecture.md", "# Architecture\n\n[Roadmap](../../docs/plan/roadmap.md)\n")
    # Site pages and code outside the docs tree.
    _write(root / "docs" / "index.md", "# Home\n")
    _write(root / "docs" / "log" / "meeting" / "m1.md", "# Meeting 1\n")
    _write(root / "docs" / "plan" / "roadmap.md", "# Roadmap\n")
    _write(root / "code" / "utils" / "detect.py", "")

    reg = {"flows": {
        name: {"name": name, "code_path": f"code/pipelines/{name}",
               "config_path": f"code/pipelines/{name}/config.yml"}
        for name in ("alpha", "beta")
    }}
    _write(root / ".projio" / "pipeio" / "registry.yml", yaml.safe_dump(reg))
    return root


def _collect(root: Path) -> list[str]:
    from pipeio.docs import docs_collect

    warnings: list[str] = []
    docs_collect(root, export=False, warnings=warnings)
    return warnings


def test_links_are_rewritten_for_the_collected_tree(tmp_path):
    root = _project(tmp_path)
    warnings = _collect(root)
    out = root / "docs" / "pipelines"

    index = (out / "alpha" / "index.md").read_text()
    assert "[detect theory](mods/detect/theory.md)" in index
    assert "[spec](mods/detect/spec.md#inputs)" in index
    assert "[CHANGELOG](changelog.md)" in index  # published for alpha
    assert "See [Snakemake report]" not in index  # no report.html anywhere
    assert "See Snakemake report and" in index
    assert "Env: pygfet" in index  # outside the repo's docs: dropped
    assert "[notes](../../log/meeting/m1.md)" in index
    assert "[site](https://example.org)" in index and "[top](#alpha)" in index
    assert "[not a link](report.md)" in index  # fenced code untouched
    assert "`[code](report.md)`" in index  # inline code untouched

    theory = (out / "alpha" / "mods" / "detect" / "theory.md").read_text()
    assert "[flow](../../index.md#alpha)" in theory
    assert "helper utils/detect.py;" in theory
    assert "[beta detect](../../../beta/mods/detect/theory.md)" in theory
    assert "![fig](fig.png)" in theory

    changelog = (out / "alpha" / "changelog.md").read_text()
    assert "[meeting](../../log/meeting/m1.md)" in changelog

    beta = (out / "beta" / "index.md").read_text()
    assert "See CHANGELOG." in beta

    arch = (out / "architecture.md").read_text()
    assert "[Roadmap](../plan/roadmap.md)" in arch

    joined = "\n".join(warnings)
    assert "report.md" in joined and "not collected" in joined
    assert "code/utils/detect.py" in joined and "outside the collected docs" in joined
    assert "envs/pygfet/index.md" in joined
    assert "docs/pipelines/beta/index.md: dropped link 'changelog.md'" in joined
    assert len(warnings) == 4


def test_collect_is_idempotent(tmp_path):
    root = _project(tmp_path)
    _collect(root)
    page = root / "docs" / "pipelines" / "alpha" / "mods" / "detect" / "theory.md"
    first = page.read_text()
    mtime = page.stat().st_mtime_ns
    assert len(_collect(root)) == 4
    assert page.read_text() == first
    assert page.stat().st_mtime_ns == mtime  # unchanged pages are not rewritten


def test_report_link_kept_when_report_collected(tmp_path):
    root = _project(tmp_path)
    _write(root / "derivatives" / "alpha" / "report.html", "<html></html>")
    warnings = _collect(root)
    index = (root / "docs" / "pipelines" / "alpha" / "index.md").read_text()
    assert "[Snakemake report](report.md)" in index
    assert not any("report.md" in w for w in warnings)


def test_mcp_docs_collect_reports_link_warnings(tmp_path):
    from pipeio.mcp import mcp_docs_collect

    root = _project(tmp_path)
    r = mcp_docs_collect(root)
    assert len(r["link_warnings"]) == 4
    assert "hint" in r


def test_strict_mkdocs_build(tmp_path):
    """The collected tree builds with ``mkdocs build --strict`` (no dead links)."""
    pytest.importorskip("mkdocs")
    root = _project(tmp_path)
    _collect(root)
    nav_extra = ["log/meeting/m1.md", "plan/roadmap.md"]
    collected = sorted(
        str(p.relative_to(root / "docs"))
        for p in (root / "docs").rglob("*.md")
    )
    cfg = {
        "site_name": "fixture",
        "docs_dir": "docs",
        "site_dir": str(tmp_path / "_site"),
        "nav": ["index.md", *(p for p in collected if p != "index.md")],
        "validation": {"links": {"not_found": "warn", "anchors": "warn",
                                 "absolute_links": "warn", "unrecognized_links": "warn"}},
    }
    assert set(nav_extra) <= set(collected)
    (root / "mkdocs.yml").write_text(yaml.safe_dump(cfg), encoding="utf-8")
    # No -q: it hides the warnings --strict counts, so the build would pass anyway.
    proc = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict"],
        cwd=root, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "WARNING" not in proc.stderr
