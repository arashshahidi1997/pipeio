"""Tests for pipeio MCP tool functions."""

from __future__ import annotations

from pathlib import Path

import yaml
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scaffold_project(root: Path) -> Path:
    """Create a minimal pipeio project with one flow, docs, and config."""
    pipeio_dir = root / ".pipeio"
    pipeio_dir.mkdir(parents=True)

    # Create a flow directory with Snakefile, config, and docs
    flow_dir = root / "code" / "pipelines" / "preproc" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(
        'rule filter_raw:\n    input: "x"\n    output: "y"\n'
        'rule filter_notch:\n    input: "a"\n    output: "b"\n',
        encoding="utf-8",
    )
    (flow_dir / "config.yml").write_text(
        "input_dir: sourcedata\noutput_dir: derivatives/preproc\n"
        "registry:\n  deriv:\n    members:\n"
        "      cleaned: {suffix: cleaned, extension: .edf}\n",
        encoding="utf-8",
    )

    docs_dir = flow_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "index.md").write_text("# Denoise\n", encoding="utf-8")
    (docs_dir / "mod-filter.md").write_text("# Filter\n", encoding="utf-8")

    # Write registry
    reg = {
        "flows": {
            "preproc/denoise": {
                "name": "denoise",
                "pipe": "preproc",
                "code_path": "code/pipelines/preproc/denoise",
                "config_path": "code/pipelines/preproc/denoise/config.yml",
            }
        }
    }
    (pipeio_dir / "registry.yml").write_text(yaml.safe_dump(reg), encoding="utf-8")

    return root


# ---------------------------------------------------------------------------
# mcp_registry_scan
# ---------------------------------------------------------------------------

def test_mcp_registry_scan(tmp_path):
    from pipeio.mcp import mcp_registry_scan

    (tmp_path / ".pipeio").mkdir()
    flow_dir = tmp_path / "code" / "pipelines" / "preproc"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()
    (flow_dir / "config.yml").write_text("output_dir: results\n", encoding="utf-8")

    result = mcp_registry_scan(tmp_path)
    assert "error" not in result
    assert result["pipes"] == 1
    assert result["flows"] == 1
    assert len(result["flow_details"]) == 1
    assert result["flow_details"][0]["pipe"] == "preproc"


def test_mcp_registry_scan_no_pipelines(tmp_path):
    from pipeio.mcp import mcp_registry_scan

    result = mcp_registry_scan(tmp_path)
    assert "error" in result


def test_mcp_registry_scan_writes_registry(tmp_path):
    from pipeio.mcp import mcp_registry_scan

    (tmp_path / ".pipeio").mkdir()
    flow_dir = tmp_path / "pipelines" / "test"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()

    mcp_registry_scan(tmp_path)
    assert (tmp_path / ".pipeio" / "registry.yml").exists()


def test_mcp_registry_scan_prefers_projio(tmp_path):
    from pipeio.mcp import mcp_registry_scan

    (tmp_path / ".projio" / "pipeio").mkdir(parents=True)
    flow_dir = tmp_path / "pipelines" / "test"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()

    mcp_registry_scan(tmp_path)
    assert (tmp_path / ".projio" / "pipeio" / "registry.yml").exists()


# ---------------------------------------------------------------------------
# mcp_docs_collect
# ---------------------------------------------------------------------------

def test_mcp_docs_collect(tmp_path):
    from pipeio.mcp import mcp_docs_collect

    _scaffold_project(tmp_path)
    result = mcp_docs_collect(tmp_path)
    assert "error" not in result
    assert result["collected"] >= 2
    assert any("index.md" in f for f in result["files"])
    assert any("mod-filter.md" in f for f in result["files"])


def test_mcp_docs_collect_no_registry(tmp_path):
    from pipeio.mcp import mcp_docs_collect

    result = mcp_docs_collect(tmp_path)
    assert result["collected"] == 0


# ---------------------------------------------------------------------------
# mcp_docs_nav
# ---------------------------------------------------------------------------

def test_mcp_docs_nav_empty(tmp_path):
    from pipeio.mcp import mcp_docs_nav

    result = mcp_docs_nav(tmp_path)
    assert "No docs/pipelines/" in result["nav_fragment"]


def test_mcp_docs_nav_with_docs(tmp_path):
    from pipeio.mcp import mcp_docs_nav

    # Create docs structure
    target = tmp_path / "docs" / "pipelines" / "preproc" / "denoise"
    target.mkdir(parents=True)
    (target / "index.md").write_text("# Denoise\n", encoding="utf-8")

    result = mcp_docs_nav(tmp_path)
    parsed = yaml.safe_load(result["nav_fragment"])
    assert parsed is not None
    assert "Pipelines" in parsed[0]


# ---------------------------------------------------------------------------
# mcp_contracts_validate
# ---------------------------------------------------------------------------

def test_mcp_contracts_validate_no_registry(tmp_path):
    from pipeio.mcp import mcp_contracts_validate

    result = mcp_contracts_validate(tmp_path)
    assert "error" in result


def test_mcp_contracts_validate_valid(tmp_path):
    from pipeio.mcp import mcp_contracts_validate

    _scaffold_project(tmp_path)
    (tmp_path / "sourcedata").mkdir()
    (tmp_path / "derivatives" / "preproc").mkdir(parents=True)

    result = mcp_contracts_validate(tmp_path)
    assert "error" not in result
    assert result["valid"] is True
    assert len(result["flows"]) == 1
    assert result["flows"][0]["flow"] == "preproc/denoise"


def test_mcp_contracts_validate_missing_dirs(tmp_path):
    from pipeio.mcp import mcp_contracts_validate

    _scaffold_project(tmp_path)
    # Don't create input/output dirs

    result = mcp_contracts_validate(tmp_path)
    assert "error" not in result
    assert len(result["flows"]) == 1
    fv = result["flows"][0]
    assert any("not found" in w for w in fv["warnings"])
