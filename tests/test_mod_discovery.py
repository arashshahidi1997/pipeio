"""Tests for mod auto-discovery from Snakefiles."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipeio.registry import PipelineRegistry, _discover_mods


# ---------------------------------------------------------------------------
# _discover_mods
# ---------------------------------------------------------------------------

def test_discover_mods_no_snakefile(tmp_path):
    assert _discover_mods(tmp_path) == {}


def test_discover_mods_empty_snakefile(tmp_path):
    (tmp_path / "Snakefile").write_text("# empty\n", encoding="utf-8")
    assert _discover_mods(tmp_path) == {}


def test_discover_mods_single_rule(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule clean_data:\n    input: 'raw.csv'\n    output: 'clean.csv'\n",
        encoding="utf-8")
    mods = _discover_mods(tmp_path)
    assert "clean" in mods
    assert mods["clean"].rules == ["clean_data"]


def test_discover_mods_groups_by_prefix(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule filter_raw:\n    pass\n\n"
        "rule filter_artifact:\n    pass\n\n"
        "rule smooth_spatial:\n    pass\n\n"
        "rule smooth_temporal:\n    pass\n",
        encoding="utf-8")
    mods = _discover_mods(tmp_path)
    assert sorted(mods.keys()) == ["filter", "smooth"]
    assert sorted(mods["filter"].rules) == ["filter_artifact", "filter_raw"]
    assert sorted(mods["smooth"].rules) == ["smooth_spatial", "smooth_temporal"]


def test_discover_mods_no_prefix(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule standalone:\n    pass\n",
        encoding="utf-8")
    mods = _discover_mods(tmp_path)
    assert "standalone" in mods
    assert mods["standalone"].rules == ["standalone"]


def test_discover_mods_smk_files(tmp_path):
    (tmp_path / "preprocess.smk").write_text(
        "rule preprocess_resample:\n    pass\n\n"
        "rule preprocess_filter:\n    pass\n",
        encoding="utf-8")
    mods = _discover_mods(tmp_path)
    assert "preprocess" in mods
    assert len(mods["preprocess"].rules) == 2


def test_discover_mods_combined_snakefile_and_smk(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule main_run:\n    pass\n",
        encoding="utf-8")
    (tmp_path / "helpers.smk").write_text(
        "rule helper_clean:\n    pass\n",
        encoding="utf-8")
    mods = _discover_mods(tmp_path)
    assert "main" in mods
    assert "helper" in mods


def test_discover_mods_with_doc_path(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule filter_raw:\n    pass\n",
        encoding="utf-8")
    # Create faceted mod doc directory with theory.md
    (tmp_path / "docs" / "filter").mkdir(parents=True)
    (tmp_path / "docs" / "filter" / "theory.md").write_text("# Filter theory\n", encoding="utf-8")

    mods = _discover_mods(tmp_path)
    assert mods["filter"].doc_path is not None
    assert mods["filter"].doc_path.endswith("/filter")


def test_discover_mods_with_spec_only(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule filter_raw:\n    pass\n",
        encoding="utf-8")
    # spec.md alone should also set doc_path
    (tmp_path / "docs" / "filter").mkdir(parents=True)
    (tmp_path / "docs" / "filter" / "spec.md").write_text("# Filter spec\n", encoding="utf-8")

    mods = _discover_mods(tmp_path)
    assert mods["filter"].doc_path is not None


def test_discover_mods_empty_doc_dir_ignored(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule filter_raw:\n    pass\n",
        encoding="utf-8")
    # Empty mod doc dir (no theory.md or spec.md) should not set doc_path
    (tmp_path / "docs" / "filter").mkdir(parents=True)

    mods = _discover_mods(tmp_path)
    assert mods["filter"].doc_path is None


def test_discover_mods_no_doc_path(tmp_path):
    (tmp_path / "Snakefile").write_text(
        "rule filter_raw:\n    pass\n",
        encoding="utf-8")
    mods = _discover_mods(tmp_path)
    assert mods["filter"].doc_path is None


# ---------------------------------------------------------------------------
# Integration: scan() populates mods
# ---------------------------------------------------------------------------

def test_scan_populates_mods(tmp_path):
    pipes_dir = tmp_path / "pipelines"
    flow_dir = pipes_dir / "preproc"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(
        "rule denoise_high:\n    pass\n\n"
        "rule denoise_low:\n    pass\n\n"
        "rule artifact_reject:\n    pass\n",
        encoding="utf-8")

    reg = PipelineRegistry.scan(pipes_dir)
    entry = reg.get("preproc")
    assert len(entry.mods) == 2
    assert "denoise" in entry.mods
    assert "artifact" in entry.mods
    assert sorted(entry.mods["denoise"].rules) == ["denoise_high", "denoise_low"]


def test_scan_mods_round_trip(tmp_path):
    """Mods should survive YAML round-trip."""
    pipes_dir = tmp_path / "pipelines"
    flow_dir = pipes_dir / "preproc"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(
        "rule denoise_high:\n    pass\n",
        encoding="utf-8")

    reg = PipelineRegistry.scan(pipes_dir)
    yaml_path = tmp_path / "registry.yml"
    reg.to_yaml(yaml_path)

    loaded = PipelineRegistry.from_yaml(yaml_path)
    entry = loaded.get("preproc")
    assert "denoise" in entry.mods
    assert entry.mods["denoise"].rules == ["denoise_high"]
