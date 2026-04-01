"""Tests for pipeio.registry."""

import textwrap
from pathlib import Path

import pytest
import yaml

from pipeio.registry import FlowEntry, ModEntry, PipelineRegistry, ValidationResult, slug_ok


# ---- slug_ok ----

@pytest.mark.parametrize(
    "name,expected",
    [
        ("preprocess", True),
        ("brain_state", True),
        ("a1", True),
        ("DGgamma", False),
        ("_private", False),
        ("1start", False),
        ("with-dash", False),
        ("", False),
    ])
def test_slug_ok(name, expected):
    assert slug_ok(name) == expected


# ---- PipelineRegistry basics ----

def _sample_registry() -> PipelineRegistry:
    return PipelineRegistry(
        flows={
            "ieeg": FlowEntry(
                name="ieeg", code_path="code/pipelines/preprocess/ieeg",
                config_path="code/pipelines/preprocess/ieeg/config.yml"),
            "ecephys": FlowEntry(
                name="ecephys", code_path="code/pipelines/preprocess/ecephys"),
            "brainstate": FlowEntry(
                name="brainstate", code_path="code/pipelines/brainstate",
                config_path="code/pipelines/brainstate/config.yml"),
        }
    )


def test_list_flows_all():
    reg = _sample_registry()
    assert len(reg.list_flows()) == 3


def test_list_flows_prefix():
    reg = _sample_registry()
    flows = reg.list_flows(prefix="brain")
    assert len(flows) == 1
    assert flows[0].name == "brainstate"


# ---- get() ----

def test_get_explicit_flow():
    reg = _sample_registry()
    entry = reg.get("ieeg")
    assert entry.name == "ieeg"


def test_get_unknown_flow():
    reg = _sample_registry()
    with pytest.raises(KeyError, match="Unknown flow"):
        reg.get("nonexistent")


# ---- remove() ----

def test_remove_flow():
    reg = _sample_registry()
    removed = reg.remove("ieeg")
    assert removed.name == "ieeg"
    assert len(reg.list_flows()) == 2


def test_remove_unknown_flow():
    reg = _sample_registry()
    with pytest.raises(KeyError, match="Flow not found"):
        reg.remove("nonexistent")


# ---- from_yaml / to_yaml round-trip ----

def test_yaml_round_trip(tmp_path):
    reg = _sample_registry()
    path = tmp_path / "registry.yml"
    reg.to_yaml(path)

    loaded = PipelineRegistry.from_yaml(path)
    assert len(loaded.list_flows()) == len(reg.list_flows())
    assert loaded.get("brainstate").name == "brainstate"


def test_yaml_backward_compat_pipe_slash_flow(tmp_path):
    """Old YAML with 'pipe/flow' keys and pipe field should load correctly."""
    old_format = {
        "flows": {
            "preprocess/ieeg": {
                "name": "ieeg",
                "pipe": "preprocess",
                "code_path": "code/pipelines/preprocess/ieeg",
            },
            "brainstate": {
                "name": "brainstate",
                "pipe": "brainstate",
                "code_path": "code/pipelines/brainstate",
            },
        }
    }
    path = tmp_path / "registry.yml"
    with open(path, "w") as fh:
        yaml.safe_dump(old_format, fh)

    loaded = PipelineRegistry.from_yaml(path)
    assert "ieeg" in loaded.flows
    assert "brainstate" in loaded.flows
    assert loaded.get("ieeg").name == "ieeg"


# ---- scan() ----

def test_scan_discovers_flows(tmp_path):
    """scan() should discover flows from Snakefile and config.yml presence."""
    pipes_dir = tmp_path / "pipelines"

    # Flat dir with Snakefile at root (flow name = dir name)
    (pipes_dir / "brainstate").mkdir(parents=True)
    (pipes_dir / "brainstate" / "Snakefile").touch()
    (pipes_dir / "brainstate" / "config.yml").touch()

    # Nested dir with sub-flows
    (pipes_dir / "preprocess" / "ieeg").mkdir(parents=True)
    (pipes_dir / "preprocess" / "ieeg" / "Snakefile").touch()
    (pipes_dir / "preprocess" / "ieeg" / "config.yml").touch()

    (pipes_dir / "preprocess" / "ecephys").mkdir(parents=True)
    (pipes_dir / "preprocess" / "ecephys" / "Snakefile").touch()

    reg = PipelineRegistry.scan(pipes_dir)
    names = sorted(f.name for f in reg.list_flows())
    assert "brainstate" in names
    assert "ieeg" in names
    assert "ecephys" in names
    assert len(reg.list_flows()) == 3


def test_scan_empty_dir(tmp_path):
    reg = PipelineRegistry.scan(tmp_path / "nonexistent")
    assert len(reg.list_flows()) == 0


def test_scan_with_docs(tmp_path):
    """scan() should pick up doc paths when docs_dir is provided."""
    pipes_dir = tmp_path / "pipelines"
    docs_dir = tmp_path / "docs"

    (pipes_dir / "brainstate").mkdir(parents=True)
    (pipes_dir / "brainstate" / "Snakefile").touch()

    # Legacy doc path format still works
    (docs_dir / "pipe-brainstate" / "flow-brainstate").mkdir(parents=True)

    reg = PipelineRegistry.scan(pipes_dir, docs_dir=docs_dir)
    entry = reg.get("brainstate")
    assert entry.doc_path is not None


# ---- validate() ----

def test_validate_clean():
    reg = _sample_registry()
    result = reg.validate()
    assert result.ok


def test_validate_bad_slug():
    reg = PipelineRegistry(
        flows={
            "bad": FlowEntry(name="BadFlow", code_path="x"),
        }
    )
    result = reg.validate()
    assert len(result.warnings) >= 1  # flow slug warning


def test_validate_missing_code_path(tmp_path):
    reg = PipelineRegistry(
        flows={
            "test": FlowEntry(
                name="test",
                code_path="nonexistent/path",
                config_path="also/nonexistent"),
        }
    )
    result = reg.validate(root=tmp_path)
    assert not result.ok
    assert any("Code path" in e for e in result.errors)
    assert any("Config path" in e for e in result.errors)


# ---- app_type detection ----

def test_registry_scan_detects_snakebids_app(tmp_path):
    """scan() sets app_type='snakebids' when run.py is present."""
    pipes_dir = tmp_path / "pipelines"
    flow_dir = pipes_dir / "preproc"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()
    (flow_dir / "run.py").touch()

    reg = PipelineRegistry.scan(pipes_dir)
    entry = reg.get("preproc")
    assert entry.app_type == "snakebids"


def test_registry_scan_detects_plain_snakemake(tmp_path):
    """scan() sets app_type='snakemake' when Snakefile exists but no run.py."""
    pipes_dir = tmp_path / "pipelines"
    flow_dir = pipes_dir / "preproc"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()

    reg = PipelineRegistry.scan(pipes_dir)
    entry = reg.get("preproc")
    assert entry.app_type == "snakemake"


def test_flow_status_includes_app_type(tmp_path):
    """mcp_flow_status returns app_type in its output dict."""
    from pipeio.mcp import mcp_registry_scan, mcp_flow_status

    (tmp_path / ".pipeio").mkdir()
    flow_dir = tmp_path / "pipelines" / "preproc"
    flow_dir.mkdir(parents=True)
    (flow_dir / "Snakefile").touch()
    (flow_dir / "run.py").touch()

    mcp_registry_scan(tmp_path)
    result = mcp_flow_status(tmp_path, "preproc")
    assert "app_type" in result
    assert result["app_type"] == "snakebids"
