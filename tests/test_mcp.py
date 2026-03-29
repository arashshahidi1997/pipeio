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


# ---------------------------------------------------------------------------
# Helpers for rule tests
# ---------------------------------------------------------------------------

_SNAKEFILE_WITH_NAMED_SECTIONS = """\
rule filter_raw:
    input:
        eeg=bids(root="raw", suffix="eeg", extension=".fif"),
    output:
        cleaned=bids(root="derivatives", suffix="cleaned", extension=".fif"),
    params:
        freq=config["filter"]["freq"],
    script:
        "scripts/filter_raw.py"

rule filter_notch:
    input:
        eeg=rules.filter_raw.output.cleaned,
    output:
        notched=bids(root="derivatives", suffix="notched", extension=".fif"),
    script:
        "scripts/filter_notch.py"
"""


def _scaffold_project_with_mods(root: Path) -> Path:
    """Minimal project with named-section Snakefile and mod-mapped registry."""
    pipeio_dir = root / ".pipeio"
    pipeio_dir.mkdir(parents=True)

    flow_dir = root / "code" / "pipelines" / "preproc" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(_SNAKEFILE_WITH_NAMED_SECTIONS, encoding="utf-8")
    (flow_dir / "config.yml").write_text(
        "input_dir: sourcedata\noutput_dir: derivatives/preproc\n"
        "registry:\n  deriv:\n    members:\n"
        "      cleaned: {suffix: cleaned, extension: .fif}\n",
        encoding="utf-8",
    )

    reg = {
        "flows": {
            "preproc/denoise": {
                "name": "denoise",
                "pipe": "preproc",
                "code_path": "code/pipelines/preproc/denoise",
                "config_path": "code/pipelines/preproc/denoise/config.yml",
                "mods": {
                    "filter": {
                        "name": "filter",
                        "rules": ["filter_raw", "filter_notch"],
                        "doc_path": None,
                    }
                },
            }
        }
    }
    (pipeio_dir / "registry.yml").write_text(yaml.safe_dump(reg), encoding="utf-8")
    return root


# ---------------------------------------------------------------------------
# mcp_rule_list
# ---------------------------------------------------------------------------

def test_mcp_rule_list_no_registry(tmp_path):
    from pipeio.mcp import mcp_rule_list

    result = mcp_rule_list(tmp_path, pipe="preproc", flow="denoise")
    assert "error" in result


def test_mcp_rule_list_returns_rules(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, pipe="preproc", flow="denoise")

    assert "error" not in result
    assert result["pipe"] == "preproc"
    assert result["flow"] == "denoise"
    assert result["rule_count"] == 2

    names = [r["name"] for r in result["rules"]]
    assert "filter_raw" in names
    assert "filter_notch" in names


def test_mcp_rule_list_named_inputs_outputs(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, pipe="preproc", flow="denoise")

    raw = next(r for r in result["rules"] if r["name"] == "filter_raw")
    assert "eeg" in raw["input"]
    assert "cleaned" in raw["output"]
    assert "freq" in raw["params"]
    assert "filter_raw.py" in raw["script"]


def test_mcp_rule_list_mod_membership_from_registry(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, pipe="preproc", flow="denoise")

    for rule in result["rules"]:
        assert rule["mod"] == "filter"


def test_mcp_rule_list_source_file_recorded(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, pipe="preproc", flow="denoise")

    for rule in result["rules"]:
        assert rule["source_file"] == "Snakefile"


# ---------------------------------------------------------------------------
# mcp_rule_stub
# ---------------------------------------------------------------------------

def test_mcp_rule_stub_no_registry(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    result = mcp_rule_stub(tmp_path, pipe="preproc", flow="denoise", rule_name="test_rule")
    assert "error" in result


def test_mcp_rule_stub_minimal(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(tmp_path, pipe="preproc", flow="denoise", rule_name="my_rule")

    assert "error" not in result
    assert result["rule_name"] == "my_rule"
    assert result["pipe"] == "preproc"
    assert result["flow"] == "denoise"
    assert "rule my_rule:" in result["stub"]


def test_mcp_rule_stub_with_inputs_outputs(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(
        tmp_path,
        pipe="preproc",
        flow="denoise",
        rule_name="clean_eeg",
        inputs={"eeg": 'bids(root="raw", suffix="eeg")'},
        outputs={"cleaned": {"root": "derivatives", "suffix": "cleaned", "extension": ".fif"}},
    )

    assert "error" not in result
    stub = result["stub"]
    assert "    input:" in stub
    assert "eeg=" in stub
    assert "    output:" in stub
    assert "cleaned=" in stub
    assert "bids(" in stub


def test_mcp_rule_stub_with_source_rule_input(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(
        tmp_path,
        pipe="preproc",
        flow="denoise",
        rule_name="filter_notch",
        inputs={"eeg": {"source_rule": "filter_raw", "member": "cleaned"}},
    )

    assert "error" not in result
    assert "rules.filter_raw.output.cleaned" in result["stub"]


def test_mcp_rule_stub_with_params_and_script(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(
        tmp_path,
        pipe="preproc",
        flow="denoise",
        rule_name="notch_filter",
        params={"freq": "filter.notch_freq"},
        script="scripts/notch_filter.py",
    )

    assert "error" not in result
    stub = result["stub"]
    assert "    params:" in stub
    assert 'config["filter"]["notch_freq"]' in stub
    assert "    script:" in stub
    assert '"scripts/notch_filter.py"' in stub


# ---------------------------------------------------------------------------
# Helpers for config read/patch tests
# ---------------------------------------------------------------------------

_CONFIG_WITH_PYBIDS = """\
input_dir: sourcedata
output_dir: derivatives/preproc

pybids_inputs:
  ieeg:
    filters:
      suffix: ieeg
    wildcards:
      - subject
      - session

registry:
  ieeg_raw:
    base_input: ieeg
    bids:
      root: output_dir
      datatype: ieeg
    members:
      fif: {suffix: ieeg, extension: .fif}
      vhdr: {suffix: ieeg, extension: .vhdr}

_member_sets:
  base_ieeg:
    suffix: ieeg
    extension: .fif
"""

_CONFIG_WITH_ANCHORS = """\
input_dir: sourcedata
output_dir: derivatives/preproc

_member_sets:
  base_ieeg: &base_ieeg
    suffix: ieeg
    extension: .fif

pybids_inputs:
  ieeg:
    wildcards:
      - subject

registry:
  ieeg_raw:
    base_input: ieeg
    bids:
      root: output_dir
    members:
      fif:
        <<: *base_ieeg
"""


def _scaffold_config_project(root: Path, config_text: str = _CONFIG_WITH_PYBIDS) -> Path:
    """Scaffold a project with a flow that has a full snakebids-style config."""
    pipeio_dir = root / ".pipeio"
    pipeio_dir.mkdir(parents=True)

    flow_dir = root / "code" / "pipelines" / "preproc" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "config.yml").write_text(config_text, encoding="utf-8")

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
# mcp_config_read
# ---------------------------------------------------------------------------

def test_mcp_config_read_no_registry(tmp_path):
    from pipeio.mcp import mcp_config_read

    result = mcp_config_read(tmp_path, pipe="preproc", flow="denoise")
    assert "error" in result


def test_mcp_config_read_returns_sections(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path)
    result = mcp_config_read(tmp_path, pipe="preproc", flow="denoise")

    assert "error" not in result
    assert result["pipe"] == "preproc"
    assert result["flow"] == "denoise"
    assert "pybids_inputs" in result
    assert "ieeg" in result["pybids_inputs"]
    assert "registry" in result
    assert "ieeg_raw" in result["registry"]
    assert "member_sets" in result
    assert "base_ieeg" in result["member_sets"]
    assert "params" in result
    assert result["params"]["input_dir"] == "sourcedata"


def test_mcp_config_read_bids_signatures(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path)
    result = mcp_config_read(tmp_path, pipe="preproc", flow="denoise")

    sigs = result["bids_signatures"]
    assert "ieeg_raw" in sigs
    assert "fif" in sigs["ieeg_raw"]
    assert "vhdr" in sigs["ieeg_raw"]

    fif_sig = sigs["ieeg_raw"]["fif"]
    assert fif_sig.startswith("bids(")
    assert 'root="output_dir"' in fif_sig
    assert 'datatype="ieeg"' in fif_sig
    assert 'suffix="ieeg"' in fif_sig
    assert 'extension=".fif"' in fif_sig
    assert "**wildcards" in fif_sig
    assert "subject" in fif_sig
    assert "session" in fif_sig


def test_mcp_config_read_has_anchors_flag(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path, _CONFIG_WITH_ANCHORS)
    result = mcp_config_read(tmp_path, pipe="preproc", flow="denoise")

    assert "error" not in result
    assert result["has_anchors"] is True


def test_mcp_config_read_no_anchors_flag(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path, _CONFIG_WITH_PYBIDS)
    result = mcp_config_read(tmp_path, pipe="preproc", flow="denoise")

    assert result["has_anchors"] is False


def test_mcp_config_read_config_path_relative(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path)
    result = mcp_config_read(tmp_path, pipe="preproc", flow="denoise")

    assert not result["config_path"].startswith("/")
    assert "config.yml" in result["config_path"]


# ---------------------------------------------------------------------------
# mcp_config_patch
# ---------------------------------------------------------------------------

def test_mcp_config_patch_no_registry(tmp_path):
    from pipeio.mcp import mcp_config_patch

    result = mcp_config_patch(tmp_path, pipe="preproc", flow="denoise")
    assert "error" in result


def test_mcp_config_patch_invalid_base_input(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    new_group = {
        "new_group": {
            "base_input": "nonexistent",
            "bids": {"root": "output_dir"},
            "members": {"out": {"suffix": "eeg", "extension": ".fif"}},
        }
    }
    result = mcp_config_patch(tmp_path, pipe="preproc", flow="denoise", registry_entry=new_group)

    assert result["valid"] is False
    assert any("nonexistent" in e for e in result["errors"])


def test_mcp_config_patch_missing_suffix(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    new_group = {
        "bad_group": {
            "base_input": "ieeg",
            "members": {"out": {"extension": ".fif"}},  # no suffix
        }
    }
    result = mcp_config_patch(tmp_path, pipe="preproc", flow="denoise", registry_entry=new_group)

    assert result["valid"] is False
    assert any("suffix" in e for e in result["errors"])


def test_mcp_config_patch_produces_diff(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    new_group = {
        "gamma_power": {
            "base_input": "ieeg",
            "bids": {"root": "output_dir", "datatype": "ieeg"},
            "members": {"npy": {"suffix": "gamma", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(tmp_path, pipe="preproc", flow="denoise", registry_entry=new_group)

    assert result["valid"] is True
    assert result["applied"] is False
    assert "gamma_power" in result["diff"]
    assert result["diff"].startswith("---")


def test_mcp_config_patch_not_applied_by_default(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    cfg_path = tmp_path / "code" / "pipelines" / "preproc" / "denoise" / "config.yml"
    original = cfg_path.read_text()

    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    mcp_config_patch(tmp_path, pipe="preproc", flow="denoise", registry_entry=new_group)

    assert cfg_path.read_text() == original


def test_mcp_config_patch_apply_writes_file(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    cfg_path = tmp_path / "code" / "pipelines" / "preproc" / "denoise" / "config.yml"

    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(
        tmp_path, pipe="preproc", flow="denoise",
        registry_entry=new_group, apply=True,
    )

    assert result["valid"] is True
    assert result["applied"] is True
    written = yaml.safe_load(cfg_path.read_text())
    assert "extra" in written["registry"]
    assert written["registry"]["extra"]["members"]["out"]["extension"] == ".npy"


def test_mcp_config_patch_params_entry(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    result = mcp_config_patch(
        tmp_path, pipe="preproc", flow="denoise",
        params_entry={"filter": {"notch_freq": 50}},
        apply=True,
    )

    assert result["valid"] is True
    cfg_path = tmp_path / "code" / "pipelines" / "preproc" / "denoise" / "config.yml"
    written = yaml.safe_load(cfg_path.read_text())
    assert written["filter"]["notch_freq"] == 50


def test_mcp_config_patch_anchor_warning(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path, _CONFIG_WITH_ANCHORS)
    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(
        tmp_path, pipe="preproc", flow="denoise", registry_entry=new_group
    )

    assert result["valid"] is True
    assert any("anchor" in w.lower() for w in result["warnings"])
