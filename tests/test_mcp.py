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
    flow_dir = root / "code" / "pipelines" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(
        'rule filter_raw:\n    input: "x"\n    output: "y"\n'
        'rule filter_notch:\n    input: "a"\n    output: "b"\n',
        encoding="utf-8")
    (flow_dir / "config.yml").write_text(
        "input_dir: sourcedata\noutput_dir: derivatives/preproc\n"
        "registry:\n  deriv:\n    members:\n"
        "      cleaned: {suffix: cleaned, extension: .edf}\n",
        encoding="utf-8")

    docs_dir = flow_dir / "docs"
    docs_dir.mkdir()
    (docs_dir / "index.md").write_text("# Denoise\n", encoding="utf-8")
    (docs_dir / "mod-filter.md").write_text("# Filter\n", encoding="utf-8")

    # Write registry
    reg = {
        "flows": {
            "denoise": {
                "name": "denoise",
                "flow": "denoise",
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
    assert result["flow_details"][0]["flow"] == "preproc" or result["flow_details"][0]["flow"] == "denoise"


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
    target = tmp_path / "docs" / "pipelines" / "denoise"
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
    assert result["flows"][0]["flow"] == "denoise"


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

    flow_dir = root / "code" / "pipelines" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(_SNAKEFILE_WITH_NAMED_SECTIONS, encoding="utf-8")
    (flow_dir / "config.yml").write_text(
        "input_dir: sourcedata\noutput_dir: derivatives/preproc\n"
        "registry:\n  deriv:\n    members:\n"
        "      cleaned: {suffix: cleaned, extension: .fif}\n",
        encoding="utf-8")

    reg = {
        "flows": {
            "denoise": {
                "name": "denoise",
                "flow": "denoise",
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

    result = mcp_rule_list(tmp_path, flow="denoise")
    assert "error" in result


def test_mcp_rule_list_returns_rules(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, flow="denoise")

    assert "error" not in result
    assert result["flow"] == "denoise"
    assert result["flow"] == "denoise"
    assert result["rule_count"] == 2

    names = [r["name"] for r in result["rules"]]
    assert "filter_raw" in names
    assert "filter_notch" in names


def test_mcp_rule_list_named_inputs_outputs(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, flow="denoise")

    raw = next(r for r in result["rules"] if r["name"] == "filter_raw")
    assert "eeg" in raw["input"]
    assert "cleaned" in raw["output"]
    assert "freq" in raw["params"]
    assert "filter_raw.py" in raw["script"]


def test_mcp_rule_list_mod_membership_from_registry(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, flow="denoise")

    for rule in result["rules"]:
        assert rule["mod"] == "filter"


def test_mcp_rule_list_source_file_recorded(tmp_path):
    from pipeio.mcp import mcp_rule_list

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_list(tmp_path, flow="denoise")

    for rule in result["rules"]:
        assert rule["source_file"] == "Snakefile"


# ---------------------------------------------------------------------------
# mcp_rule_stub
# ---------------------------------------------------------------------------

def test_mcp_rule_stub_no_registry(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    result = mcp_rule_stub(tmp_path, flow="denoise", rule_name="test_rule")
    assert "error" in result


def test_mcp_rule_stub_minimal(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(tmp_path, flow="denoise", rule_name="my_rule")

    assert "error" not in result
    assert result["rule_name"] == "my_rule"
    assert result["flow"] == "denoise"
    assert result["flow"] == "denoise"
    assert "rule my_rule:" in result["stub"]


def test_mcp_rule_stub_with_inputs_outputs(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(
        tmp_path,
        flow="denoise",
        rule_name="clean_eeg",
        inputs={"eeg": 'bids(root="raw", suffix="eeg")'},
        outputs={"cleaned": {"root": "derivatives", "suffix": "cleaned", "extension": ".fif"}})

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
        flow="denoise",
        rule_name="filter_notch",
        inputs={"eeg": {"source_rule": "filter_raw", "member": "cleaned"}})

    assert "error" not in result
    assert "rules.filter_raw.output.cleaned" in result["stub"]


def test_mcp_rule_stub_with_params_and_script(tmp_path):
    from pipeio.mcp import mcp_rule_stub

    _scaffold_project_with_mods(tmp_path)
    result = mcp_rule_stub(
        tmp_path,
        flow="denoise",
        rule_name="notch_filter",
        params={"freq": "filter.notch_freq"},
        script="scripts/notch_filter.py")

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

    flow_dir = root / "code" / "pipelines" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "config.yml").write_text(config_text, encoding="utf-8")

    reg = {
        "flows": {
            "denoise": {
                "name": "denoise",
                "flow": "denoise",
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

    result = mcp_config_read(tmp_path, flow="denoise")
    assert "error" in result


def test_mcp_config_read_returns_sections(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path)
    result = mcp_config_read(tmp_path, flow="denoise")

    assert "error" not in result
    assert result["flow"] == "denoise"
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
    result = mcp_config_read(tmp_path, flow="denoise")

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
    result = mcp_config_read(tmp_path, flow="denoise")

    assert "error" not in result
    assert result["has_anchors"] is True


def test_mcp_config_read_no_anchors_flag(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path, _CONFIG_WITH_PYBIDS)
    result = mcp_config_read(tmp_path, flow="denoise")

    assert result["has_anchors"] is False


def test_mcp_config_read_config_path_relative(tmp_path):
    from pipeio.mcp import mcp_config_read

    _scaffold_config_project(tmp_path)
    result = mcp_config_read(tmp_path, flow="denoise")

    assert not result["config_path"].startswith("/")
    assert "config.yml" in result["config_path"]


# ---------------------------------------------------------------------------
# mcp_config_patch
# ---------------------------------------------------------------------------

def test_mcp_config_patch_no_registry(tmp_path):
    from pipeio.mcp import mcp_config_patch

    result = mcp_config_patch(tmp_path, flow="denoise")
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
    result = mcp_config_patch(tmp_path, flow="denoise", registry_entry=new_group)

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
    result = mcp_config_patch(tmp_path, flow="denoise", registry_entry=new_group)

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
    result = mcp_config_patch(tmp_path, flow="denoise", registry_entry=new_group)

    assert result["valid"] is True
    assert result["applied"] is False
    assert "gamma_power" in result["diff"]
    assert result["diff"].startswith("---")


def test_mcp_config_patch_not_applied_by_default(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    cfg_path = tmp_path / "code" / "pipelines" / "denoise" / "config.yml"
    original = cfg_path.read_text()

    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    mcp_config_patch(tmp_path, flow="denoise", registry_entry=new_group)

    assert cfg_path.read_text() == original


def test_mcp_config_patch_apply_writes_file(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    cfg_path = tmp_path / "code" / "pipelines" / "denoise" / "config.yml"

    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(
        tmp_path, flow="denoise",
        registry_entry=new_group, apply=True)

    assert result["valid"] is True
    assert result["applied"] is True
    written = yaml.safe_load(cfg_path.read_text())
    assert "extra" in written["registry"]
    assert written["registry"]["extra"]["members"]["out"]["extension"] == ".npy"


def test_mcp_config_patch_params_entry(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path)
    result = mcp_config_patch(
        tmp_path, flow="denoise",
        params_entry={"filter": {"notch_freq": 50}},
        apply=True)

    assert result["valid"] is True
    cfg_path = tmp_path / "code" / "pipelines" / "denoise" / "config.yml"
    written = yaml.safe_load(cfg_path.read_text())
    assert written["filter"]["notch_freq"] == 50


def test_mcp_config_patch_preserves_anchors(tmp_path):
    from pipeio.mcp import mcp_config_patch

    _scaffold_config_project(tmp_path, _CONFIG_WITH_ANCHORS)
    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(
        tmp_path, flow="denoise",
        registry_entry=new_group, apply=True)

    assert result["valid"] is True
    assert result["applied"] is True
    # No anchor warnings — ruamel round-trip preserves them
    assert not result["warnings"]

    # Verify anchors and aliases survive the round-trip
    cfg_path = tmp_path / "code" / "pipelines" / "denoise" / "config.yml"
    written = cfg_path.read_text()
    assert "&base_ieeg" in written
    assert "*base_ieeg" in written


def test_mcp_config_patch_preserves_comments(tmp_path):
    from pipeio.mcp import mcp_config_patch

    config_with_comments = """\
# Pipeline configuration
input_dir: sourcedata
output_dir: derivatives/preproc  # output location

pybids_inputs:
  ieeg:
    wildcards:
      - subject
      - session

# Output registry
registry:
  ieeg_raw:
    base_input: ieeg
    bids:
      root: output_dir
      datatype: ieeg
    members:
      fif: {suffix: ieeg, extension: .fif}
"""
    _scaffold_config_project(tmp_path, config_with_comments)
    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(
        tmp_path, flow="denoise",
        registry_entry=new_group, apply=True)

    assert result["valid"] is True
    cfg_path = tmp_path / "code" / "pipelines" / "denoise" / "config.yml"
    written = cfg_path.read_text()
    assert "# Pipeline configuration" in written
    assert "# output location" in written
    assert "# Output registry" in written


def test_mcp_config_patch_preserves_unreferenced_anchors(tmp_path):
    from pipeio.mcp import mcp_config_patch

    config_with_unreferenced = """\
_member_sets:
  log_default: &log_default
    suffix: log
    extension: .txt
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
    _scaffold_config_project(tmp_path, config_with_unreferenced)
    new_group = {
        "extra": {
            "base_input": "ieeg",
            "members": {"out": {"suffix": "out", "extension": ".npy"}},
        }
    }
    result = mcp_config_patch(
        tmp_path, flow="denoise",
        registry_entry=new_group, apply=True)

    assert result["valid"] is True
    cfg_path = tmp_path / "code" / "pipelines" / "denoise" / "config.yml"
    written = cfg_path.read_text()
    # Referenced anchor preserved
    assert "&base_ieeg" in written
    assert "*base_ieeg" in written
    # Unreferenced anchor also preserved
    assert "&log_default" in written


# ---------------------------------------------------------------------------
# _config_path_to_expr passthrough
# ---------------------------------------------------------------------------

def test_config_path_to_expr_passthrough():
    from pipeio.mcp import _config_path_to_expr

    # Dot-path gets converted
    assert _config_path_to_expr("ttl_removal.ttl_freq") == 'config["ttl_removal"]["ttl_freq"]'

    # Already a config expression — passed through verbatim
    raw = 'config["ttl_removal"]["ttl_freq"]'
    assert _config_path_to_expr(raw) == raw

    # Expression with function call — passed through
    assert _config_path_to_expr("int(config['x'])") == "int(config['x'])"


# ---------------------------------------------------------------------------
# mcp_nb_update
# ---------------------------------------------------------------------------

def _scaffold_notebook_project(root: Path) -> Path:
    """Scaffold project with a flow that has a notebook.yml."""
    _scaffold_project(root)
    flow_dir = root / "code" / "pipelines" / "denoise"
    nb_dir = flow_dir / "notebooks"
    nb_dir.mkdir(parents=True, exist_ok=True)

    (nb_dir / "investigate_noise.py").write_text("# %% [markdown]\n# Hello\n")
    nb_cfg = {
        "publish": {"format": "html", "docs_dir": "", "prefix": ""},
        "entries": [
            {
                "path": "notebooks/investigate_noise.py",
                "kind": "investigate",
                "description": "Check noise patterns",
                "status": "active",
                "pair_ipynb": True,
                "pair_myst": False,
                "publish_myst": False,
                "publish_html": False,
            }
        ],
    }
    (nb_dir / "notebook.yml").write_text(
        yaml.safe_dump(nb_cfg, sort_keys=False), encoding="utf-8"
    )
    return root


def test_mcp_nb_status_includes_metadata(tmp_path):
    from pipeio.mcp import mcp_nb_status

    _scaffold_notebook_project(tmp_path)
    result = mcp_nb_status(tmp_path)

    assert result["flows"]
    nb = result["flows"][0]["notebooks"][0]
    assert nb["name"] == "investigate_noise"
    assert nb["kind"] == "investigate"
    assert nb["description"] == "Check noise patterns"
    assert nb["status"] == "active"


def test_mcp_nb_update_changes_status(tmp_path):
    from pipeio.mcp import mcp_nb_update

    _scaffold_notebook_project(tmp_path)
    result = mcp_nb_update(
        tmp_path, flow="denoise",
        name="investigate_noise", status="stale")

    assert result["updated_fields"] == ["status"]
    assert result["entry"]["status"] == "stale"

    # Verify persisted
    from pipeio.notebook.config import NotebookConfig
    nb_cfg_path = (
        tmp_path / "code" / "pipelines" / "denoise"
        / "notebooks" / "notebook.yml"
    )
    nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
    assert nb_cfg.entries[0].status == "stale"


def test_mcp_nb_update_preserves_other_fields(tmp_path):
    from pipeio.mcp import mcp_nb_update

    _scaffold_notebook_project(tmp_path)
    result = mcp_nb_update(
        tmp_path, flow="denoise",
        name="investigate_noise", description="Updated description")

    assert result["entry"]["description"] == "Updated description"
    assert result["entry"]["kind"] == "investigate"
    assert result["entry"]["status"] == "active"
    assert result["entry"]["pair_ipynb"] is True


def test_mcp_nb_update_not_found(tmp_path):
    from pipeio.mcp import mcp_nb_update

    _scaffold_notebook_project(tmp_path)
    result = mcp_nb_update(
        tmp_path, flow="denoise",
        name="nonexistent", status="stale")

    assert "error" in result


def test_mcp_nb_update_no_fields(tmp_path):
    from pipeio.mcp import mcp_nb_update

    _scaffold_notebook_project(tmp_path)
    result = mcp_nb_update(
        tmp_path, flow="denoise",
        name="investigate_noise")

    assert "error" in result


def test_mcp_nb_create_persists_metadata(tmp_path):
    from pipeio.mcp import mcp_nb_create

    _scaffold_project(tmp_path)
    result = mcp_nb_create(
        tmp_path, flow="denoise",
        name="explore_lfp", kind="explore",
        description="Explore LFP band power")

    assert "error" not in result

    from pipeio.notebook.config import NotebookConfig
    nb_cfg_path = (
        tmp_path / "code" / "pipelines" / "denoise"
        / "notebooks" / "notebook.yml"
    )
    nb_cfg = NotebookConfig.from_yaml(nb_cfg_path)
    entry = nb_cfg.entries[0]
    assert entry.kind == "explore"
    assert entry.description == "Explore LFP band power"
    assert entry.status == "active"


# ---------------------------------------------------------------------------
# mcp_mod_context
# ---------------------------------------------------------------------------

def _scaffold_mod_context_project(root: Path) -> Path:
    """Scaffold project with mod, scripts, docs, and config for mod_context tests."""
    pipeio_dir = root / ".pipeio"
    pipeio_dir.mkdir(parents=True)

    flow_dir = root / "code" / "pipelines" / "denoise"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").write_text(_SNAKEFILE_WITH_NAMED_SECTIONS, encoding="utf-8")

    # Config with filter params
    (flow_dir / "config.yml").write_text(
        "input_dir: sourcedata\n"
        "output_dir: derivatives/preproc\n"
        "filter:\n"
        "  freq: 50\n"
        "  order: 4\n"
        "pybids_inputs:\n"
        "  eeg:\n"
        "    wildcards:\n"
        "      - subject\n"
        "registry:\n"
        "  deriv:\n"
        "    base_input: eeg\n"
        "    bids:\n"
        "      root: derivatives\n"
        "    members:\n"
        "      cleaned: {suffix: cleaned, extension: .fif}\n",
        encoding="utf-8")

    # Scripts
    scripts_dir = flow_dir / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "filter_raw.py").write_text(
        "import mne\n# filter raw data\n", encoding="utf-8"
    )
    (scripts_dir / "filter_notch.py").write_text(
        "import mne\n# notch filter\n", encoding="utf-8"
    )

    # Mod doc
    doc_dir = root / "docs" / "pipelines" / "pipe-preproc" / "flow-denoise" / "mod-filter"
    doc_dir.mkdir(parents=True)
    (doc_dir / "index.md").write_text("# Filter Module\nApplies filtering.\n", encoding="utf-8")

    reg = {
        "flows": {
            "denoise": {
                "name": "denoise",
                "flow": "denoise",
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


def test_mcp_mod_context_returns_rules(tmp_path):
    from pipeio.mcp import mcp_mod_context

    _scaffold_mod_context_project(tmp_path)
    result = mcp_mod_context(tmp_path, flow="denoise", mod="filter")

    assert "error" not in result
    assert result["mod"] == "filter"
    assert len(result["rules"]) == 2
    rule_names = {r["name"] for r in result["rules"]}
    assert rule_names == {"filter_raw", "filter_notch"}


def test_mcp_mod_context_reads_scripts(tmp_path):
    from pipeio.mcp import mcp_mod_context

    _scaffold_mod_context_project(tmp_path)
    result = mcp_mod_context(tmp_path, flow="denoise", mod="filter")

    assert "scripts/filter_raw.py" in result["scripts"]
    assert "import mne" in result["scripts"]["scripts/filter_raw.py"]
    assert "scripts/filter_notch.py" in result["scripts"]


def test_mcp_mod_context_reads_doc(tmp_path):
    from pipeio.mcp import mcp_mod_context

    _scaffold_mod_context_project(tmp_path)
    result = mcp_mod_context(tmp_path, flow="denoise", mod="filter")

    assert result["doc"] is not None
    assert "Filter Module" in result["doc"]


def test_mcp_mod_context_extracts_config_params(tmp_path):
    from pipeio.mcp import mcp_mod_context

    _scaffold_mod_context_project(tmp_path)
    result = mcp_mod_context(tmp_path, flow="denoise", mod="filter")

    assert "filter" in result["config_params"]
    assert result["config_params"]["filter"]["freq"] == 50


def test_mcp_mod_context_missing_mod(tmp_path):
    from pipeio.mcp import mcp_mod_context

    _scaffold_mod_context_project(tmp_path)
    result = mcp_mod_context(tmp_path, flow="denoise", mod="nonexistent")

    assert "error" in result


def test_mcp_mod_context_bids_signatures(tmp_path):
    from pipeio.mcp import mcp_mod_context

    _scaffold_mod_context_project(tmp_path)
    result = mcp_mod_context(tmp_path, flow="denoise", mod="filter")

    assert "deriv" in result["bids_signatures"]
    assert "cleaned" in result["bids_signatures"]["deriv"]
