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
    ],
)
def test_slug_ok(name, expected):
    assert slug_ok(name) == expected


# ---- PipelineRegistry basics ----

def _sample_registry() -> PipelineRegistry:
    return PipelineRegistry(
        flows={
            "preprocess/ieeg": FlowEntry(
                name="ieeg", pipe="preprocess", code_path="code/pipelines/preprocess/ieeg",
                config_path="code/pipelines/preprocess/ieeg/config.yml",
            ),
            "preprocess/ecephys": FlowEntry(
                name="ecephys", pipe="preprocess", code_path="code/pipelines/preprocess/ecephys",
            ),
            "brainstate": FlowEntry(
                name="brainstate", pipe="brainstate", code_path="code/pipelines/brainstate",
                config_path="code/pipelines/brainstate/config.yml",
            ),
        }
    )


def test_list_pipes():
    reg = _sample_registry()
    assert reg.list_pipes() == ["brainstate", "preprocess"]


def test_list_flows_all():
    reg = _sample_registry()
    assert len(reg.list_flows()) == 3


def test_list_flows_filtered():
    reg = _sample_registry()
    flows = reg.list_flows(pipe="preprocess")
    assert len(flows) == 2
    assert all(f.pipe == "preprocess" for f in flows)


# ---- get() ----

def test_get_explicit_flow():
    reg = _sample_registry()
    entry = reg.get("preprocess", "ieeg")
    assert entry.name == "ieeg"
    assert entry.pipe == "preprocess"


def test_get_auto_single_flow():
    """Pipe with one flow should auto-select."""
    reg = _sample_registry()
    entry = reg.get("brainstate")
    assert entry.name == "brainstate"


def test_get_auto_pipe_eq_flow():
    """When flow is omitted but a flow with the same name as the pipe exists."""
    reg = PipelineRegistry(
        flows={
            "myp/alpha": FlowEntry(name="alpha", pipe="myp", code_path="a"),
            "myp/myp": FlowEntry(name="myp", pipe="myp", code_path="b"),
        }
    )
    entry = reg.get("myp")
    assert entry.name == "myp"


def test_get_unknown_pipe():
    reg = _sample_registry()
    with pytest.raises(KeyError, match="Unknown pipe"):
        reg.get("nonexistent")


def test_get_unknown_flow():
    reg = _sample_registry()
    with pytest.raises(KeyError, match="Unknown flow"):
        reg.get("preprocess", "nonexistent")


def test_get_ambiguous_flow():
    reg = _sample_registry()
    with pytest.raises(ValueError, match="flow is required"):
        reg.get("preprocess")


# ---- from_yaml / to_yaml round-trip ----

def test_yaml_round_trip(tmp_path):
    reg = _sample_registry()
    path = tmp_path / "registry.yml"
    reg.to_yaml(path)

    loaded = PipelineRegistry.from_yaml(path)
    assert loaded.list_pipes() == reg.list_pipes()
    assert len(loaded.list_flows()) == len(reg.list_flows())
    assert loaded.get("brainstate").name == "brainstate"


# ---- scan() ----

def test_scan_discovers_flows(tmp_path):
    """scan() should discover flows from Snakefile and config.yml presence."""
    pipes_dir = tmp_path / "pipelines"

    # pipe with Snakefile at root (single flow = pipe name)
    (pipes_dir / "brainstate").mkdir(parents=True)
    (pipes_dir / "brainstate" / "Snakefile").touch()
    (pipes_dir / "brainstate" / "config.yml").touch()

    # pipe with sub-flows
    (pipes_dir / "preprocess" / "ieeg").mkdir(parents=True)
    (pipes_dir / "preprocess" / "ieeg" / "Snakefile").touch()
    (pipes_dir / "preprocess" / "ieeg" / "config.yml").touch()

    (pipes_dir / "preprocess" / "ecephys").mkdir(parents=True)
    (pipes_dir / "preprocess" / "ecephys" / "Snakefile").touch()

    reg = PipelineRegistry.scan(pipes_dir)
    assert "brainstate" in reg.list_pipes()
    assert "preprocess" in reg.list_pipes()
    assert len(reg.list_flows(pipe="preprocess")) == 2


def test_scan_empty_dir(tmp_path):
    reg = PipelineRegistry.scan(tmp_path / "nonexistent")
    assert len(reg.list_flows()) == 0


def test_scan_with_docs(tmp_path):
    """scan() should pick up doc paths when docs_dir is provided."""
    pipes_dir = tmp_path / "pipelines"
    docs_dir = tmp_path / "docs"

    (pipes_dir / "brainstate").mkdir(parents=True)
    (pipes_dir / "brainstate" / "Snakefile").touch()

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
            "bad": FlowEntry(name="BadFlow", pipe="GoodPipe", code_path="x"),
        }
    )
    result = reg.validate()
    assert len(result.warnings) >= 2  # both pipe and flow slugs


def test_validate_missing_code_path(tmp_path):
    reg = PipelineRegistry(
        flows={
            "test": FlowEntry(
                name="test", pipe="test",
                code_path="nonexistent/path",
                config_path="also/nonexistent",
            ),
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
    result = mcp_flow_status(tmp_path, "preproc", "preproc")
    assert "app_type" in result
    assert result["app_type"] == "snakebids"
