"""Tests for pipeio docs collection and init consolidation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import yaml
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _scaffold_project(root: Path, *, under_projio: bool = False) -> Path:
    """Create a minimal pipeio project with one flow and registry."""
    if under_projio:
        pipeio_dir = root / ".projio" / "pipeio"
    else:
        pipeio_dir = root / ".pipeio"
    pipeio_dir.mkdir(parents=True)

    # Create a flow directory
    flow_dir = root / "code" / "pipelines" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()
    (flow_dir / "config.yml").write_text("output_dir: results\n", encoding="utf-8")

    # Create flow-local docs
    docs_dir = flow_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "index.md").write_text("# Denoise\nOverview of denoise flow.\n", encoding="utf-8")
    (docs_dir / "mod-smoothing.md").write_text("# Smoothing Mod\n", encoding="utf-8")

    # Create notebook config + py file
    nb_dir = flow_dir / "notebooks"
    nb_dir.mkdir()
    nb_cfg = {"publish": {"format": "html"},
        "entries": [
            {"path": "notebooks/analysis.py",
                "pair_ipynb": True,
                "publish_html": True,
            }
        ],
    }
    (nb_dir / "notebook.yml").write_text(yaml.safe_dump(nb_cfg), encoding="utf-8")
    (nb_dir / "analysis.py").write_text("# %% [markdown]\n# Analysis\n# %%\nx = 1\n", encoding="utf-8")

    # Write registry
    reg = {"flows": {"denoise": {"name": "denoise",
                "code_path": "code/pipelines/denoise",
                "config_path": "code/pipelines/denoise/config.yml",
            }
        }
    }
    (pipeio_dir / "registry.yml").write_text(yaml.safe_dump(reg), encoding="utf-8")

    return root


# ---------------------------------------------------------------------------
# _find_registry
# ---------------------------------------------------------------------------

def test_find_registry_projio(tmp_path):
    _scaffold_project(tmp_path, under_projio=True)
    from pipeio.cli import _find_registry
    reg = _find_registry(tmp_path)
    assert reg is not None
    assert ".projio" in str(reg)


def test_find_registry_legacy(tmp_path):
    _scaffold_project(tmp_path, under_projio=False)
    from pipeio.cli import _find_registry
    reg = _find_registry(tmp_path)
    assert reg is not None
    assert ".pipeio" in str(reg)


def test_find_registry_none(tmp_path):
    from pipeio.cli import _find_registry
    assert _find_registry(tmp_path) is None


def test_find_registry_prefers_projio(tmp_path):
    """When both .projio/pipeio/ and .pipeio/ exist, prefer .projio/."""
    _scaffold_project(tmp_path, under_projio=True)
    # Also create legacy
    legacy = tmp_path / ".pipeio"
    legacy.mkdir()
    (legacy / "registry.yml").write_text("flows: {}\n", encoding="utf-8")

    from pipeio.cli import _find_registry
    reg = _find_registry(tmp_path)
    assert ".projio" in str(reg)


# ---------------------------------------------------------------------------
# _pipeio_dir / init consolidation
# ---------------------------------------------------------------------------

def test_pipeio_dir_standalone(tmp_path):
    from pipeio.cli import _pipeio_dir
    # No .projio/ directory — should fall back to .pipeio/
    assert _pipeio_dir(tmp_path) == tmp_path / ".pipeio"


def test_pipeio_dir_under_projio(tmp_path):
    (tmp_path / ".projio").mkdir()
    from pipeio.cli import _pipeio_dir
    assert _pipeio_dir(tmp_path) == tmp_path / ".projio" / "pipeio"


def test_init_under_projio(tmp_path, capsys):
    (tmp_path / ".projio").mkdir()
    from pipeio.cli import main
    ret = main(["init", "--root", str(tmp_path)])
    assert ret == 0
    assert (tmp_path / ".projio" / "pipeio" / "registry.yml").exists()
    assert not (tmp_path / ".pipeio").exists()


def test_init_standalone(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["init", "--root", str(tmp_path)])
    assert ret == 0
    assert (tmp_path / ".pipeio" / "registry.yml").exists()


def test_init_idempotent_projio(tmp_path, capsys):
    (tmp_path / ".projio").mkdir()
    from pipeio.cli import main
    main(["init", "--root", str(tmp_path)])
    capsys.readouterr()
    ret = main(["init", "--root", str(tmp_path)])
    assert ret == 0
    assert "already initialized" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# docs_collect
# ---------------------------------------------------------------------------

def test_docs_collect_copies_flow_docs(tmp_path):
    _scaffold_project(tmp_path)
    from pipeio.docs import docs_collect
    collected = docs_collect(tmp_path)
    # Should have collected index.md and mod-smoothing.md
    doc_files = [p for p in collected if p.endswith(".md")]
    assert len(doc_files) == 2
    target = tmp_path / "docs" / "pipelines" / "denoise"
    assert (target / "index.md").exists()
    assert (target / "mod-smoothing.md").exists()


def test_docs_collect_faceted_mod_docs(tmp_path):
    """Faceted mod docs (theory.md, spec.md) go to mods/{mod}/ in published docs."""
    _scaffold_project(tmp_path)
    # Add faceted docs
    flow_dir = tmp_path / "code" / "pipelines" / "denoise"
    facet_dir = flow_dir / "docs" / "filter"
    facet_dir.mkdir(parents=True, exist_ok=True)
    (facet_dir / "theory.md").write_text("# Theory\n", encoding="utf-8")
    (facet_dir / "spec.md").write_text("# Spec\n", encoding="utf-8")

    from pipeio.docs import docs_collect
    collected = docs_collect(tmp_path)

    target = tmp_path / "docs" / "pipelines" / "denoise"
    # Faceted docs should be under mods/
    assert (target / "mods" / "filter" / "theory.md").exists()
    assert (target / "mods" / "filter" / "spec.md").exists()
    # Flow-level docs still at root
    assert (target / "index.md").exists()


def test_docs_collect_publish_yml(tmp_path):
    """publish.yml controls DAG, report, and scripts collection."""
    _scaffold_project(tmp_path)
    flow_dir = tmp_path / "code" / "pipelines" / "denoise"

    # Create publish.yml
    (flow_dir / "publish.yml").write_text(
        "dag: true\nreport: true\nscripts: true\n", encoding="utf-8")

    # Create artifacts
    (flow_dir / "dag.svg").write_text("<svg/>", encoding="utf-8")
    (flow_dir / "report.html").write_text("<html/>", encoding="utf-8")
    scripts_dir = flow_dir / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "filter.py").write_text('"""Apply bandpass filter."""\n', encoding="utf-8")

    from pipeio.docs import docs_collect
    collected = docs_collect(tmp_path)

    target = tmp_path / "docs" / "pipelines" / "denoise"
    assert (target / "dag.svg").exists()
    assert (target / "report.html").exists()
    assert (target / "scripts.md").exists()
    scripts_md = (target / "scripts.md").read_text()
    assert "filter.py" in scripts_md
    assert "bandpass" in scripts_md


def test_docs_collect_no_registry(tmp_path):
    from pipeio.docs import docs_collect
    assert docs_collect(tmp_path) == []


def test_docs_collect_projio_registry(tmp_path):
    _scaffold_project(tmp_path, under_projio=True)
    from pipeio.docs import docs_collect
    collected = docs_collect(tmp_path)
    assert len(collected) >= 2  # at least the .md files


def test_docs_collect_publishes_html(tmp_path):
    """When ipynb exists, docs collect should call nbconvert for HTML."""
    _scaffold_project(tmp_path)

    # Create a fake .ipynb so publish has something to work with
    import json
    flow_dir = tmp_path / "code" / "pipelines" / "denoise"
    ipynb = flow_dir / "notebooks" / "analysis.ipynb"
    nb_data = {"nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"kernelspec": {"name": "python3"}},
        "cells": [{"cell_type": "code", "source": "x = 1", "metadata": {}, "outputs": [], "execution_count": None}],
    }
    ipynb.write_text(json.dumps(nb_data), encoding="utf-8")

    calls = []

    def fake_run(cmd, check):
        calls.append(cmd)
        # Simulate nbconvert creating the HTML file
        if "--to" in cmd and "html" in cmd:
            out_dir_idx = cmd.index("--output-dir") + 1
            out_name_idx = cmd.index("--output") + 1
            out = Path(cmd[out_dir_idx]) / cmd[out_name_idx]
            out.parent.mkdir(parents=True, exist_ok=True)
            out.touch()

    with patch("pipeio.docs.subprocess.run", side_effect=fake_run):
        from pipeio.docs import docs_collect
        collected = docs_collect(tmp_path)

    html_files = [p for p in collected if p.endswith(".html")]
    assert len(html_files) == 1
    assert "analysis.html" in html_files[0]
    assert len(calls) == 1
    assert "html" in calls[0]


# ---------------------------------------------------------------------------
# docs_nav
# ---------------------------------------------------------------------------

def test_docs_nav_empty(tmp_path):
    from pipeio.docs import docs_nav
    result = docs_nav(tmp_path)
    assert "No docs/pipelines/" in result


def test_docs_nav_generates_yaml(tmp_path):
    # Create the docs/pipelines structure manually
    target = tmp_path / "docs" / "pipelines" / "denoise"
    target.mkdir(parents=True)
    (target / "index.md").write_text("# Denoise\n", encoding="utf-8")
    (target / "mod-smoothing.md").write_text("# Smoothing\n", encoding="utf-8")

    nb_dir = target / "notebooks"
    nb_dir.mkdir()
    (nb_dir / "analysis.html").touch()

    from pipeio.docs import docs_nav
    result = docs_nav(tmp_path)
    parsed = yaml.safe_load(result)
    assert parsed is not None
    assert isinstance(parsed, list)
    assert "Pipelines" in parsed[0]


def test_docs_nav_includes_notebooks(tmp_path):
    flow_dir = tmp_path / "docs" / "pipelines" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "index.md").write_text("# Denoise\n", encoding="utf-8")
    nb_dir = flow_dir / "notebooks"
    nb_dir.mkdir()
    (nb_dir / "analysis.html").touch()

    from pipeio.docs import docs_nav
    result = docs_nav(tmp_path)
    assert "analysis" in result.lower()


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

def test_cli_docs_collect_no_registry(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["docs", "--root", str(tmp_path), "collect"])
    assert ret == 0
    assert "Nothing to collect" in capsys.readouterr().out


def test_cli_docs_collect_with_project(tmp_path, capsys):
    _scaffold_project(tmp_path)
    from pipeio.cli import main
    ret = main(["docs", "--root", str(tmp_path), "collect"])
    assert ret == 0
    out = capsys.readouterr().out
    assert "collected" in out


def test_cli_docs_nav(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["docs", "--root", str(tmp_path), "nav"])
    assert ret == 0


def test_cli_docs_no_subcommand(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["docs", "--root", str(tmp_path)])
    assert ret == 0


def test_cli_flow_list_uses_find_registry(tmp_path, capsys):
    """flow list should find the registry under .projio/pipeio/."""
    _scaffold_project(tmp_path, under_projio=True)
    from pipeio.cli import main
    capsys.readouterr()
    ret = main(["flow", "--root", str(tmp_path), "list"])
    assert ret == 0
    assert "denoise" in capsys.readouterr().out


def test_cli_registry_validate_projio(tmp_path, capsys):
    _scaffold_project(tmp_path, under_projio=True)
    from pipeio.cli import main
    ret = main(["registry", "--root", str(tmp_path), "validate"])
    assert ret == 0


def test_cli_registry_scan_output_projio(tmp_path, capsys):
    """registry scan should write to .projio/pipeio/ when .projio/ exists."""
    (tmp_path / ".projio").mkdir()
    pipes_dir = tmp_path / "pipelines" / "test"
    pipes_dir.mkdir(parents=True)
    (pipes_dir / "Snakefile").touch()

    from pipeio.cli import main
    ret = main(["registry", "--root", str(tmp_path), "scan", "--pipelines-dir", str(tmp_path / "pipelines")])
    assert ret == 0
    assert (tmp_path / ".projio" / "pipeio" / "registry.yml").exists()
