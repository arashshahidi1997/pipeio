"""Tests for pipeio.cli base/workflow snakemake profile injection (_profile_args)."""
from pathlib import Path

import yaml

from pipeio.cli import _profile_args


def _mk_profile(d: Path):
    d.mkdir(parents=True, exist_ok=True)
    (d / "config.yaml").write_text("cores: 1\n")


def _project(tmp_path, *, base_rel="_profiles/base", with_base=True, with_flow_override=True):
    root = tmp_path
    (root / ".projio" / "pipeio").mkdir(parents=True, exist_ok=True)
    if with_base:
        (root / ".projio" / "pipeio" / "config.yml").write_text(
            yaml.safe_dump({"profiles": {"base": base_rel}}))
        _mk_profile(root / base_rel)
    flow_dir = root / "code" / "pipelines" / "myflow"
    flow_dir.mkdir(parents=True, exist_ok=True)
    if with_flow_override:
        _mk_profile(flow_dir / "profiles" / "default")
    return root, flow_dir


def test_injects_base_and_workflow(tmp_path):
    root, flow_dir = _project(tmp_path)
    assert _profile_args(root, flow_dir, []) == [
        "--profile", str(root / "_profiles/base"),
        "--workflow-profile", str(flow_dir / "profiles" / "default"),
    ]


def test_user_profile_wins(tmp_path):
    root, flow_dir = _project(tmp_path)
    args = _profile_args(root, flow_dir, ["--profile", "/custom", "-j1"])
    assert "--profile" not in args  # user's base override respected
    assert args == ["--workflow-profile", str(flow_dir / "profiles" / "default")]


def test_user_workflow_profile_wins(tmp_path):
    root, flow_dir = _project(tmp_path)
    args = _profile_args(root, flow_dir, ["--workflow-profile", "/wf"])
    assert "--workflow-profile" not in args
    assert args == ["--profile", str(root / "_profiles/base")]


def test_nothing_configured(tmp_path):
    root, flow_dir = _project(tmp_path, with_base=False, with_flow_override=False)
    assert _profile_args(root, flow_dir, []) == []


def test_base_dir_without_config_skipped(tmp_path):
    root, flow_dir = _project(tmp_path, with_flow_override=False)
    (root / "_profiles/base" / "config.yaml").unlink()  # dir exists but not a valid profile
    assert _profile_args(root, flow_dir, []) == []


def _registered(root):
    flow_dir = root / "code" / "pipelines" / "myflow"
    (flow_dir / "Snakefile").write_text("rule all:\n    input: []\n")
    (root / ".projio" / "pipeio" / "registry.yml").write_text(yaml.safe_dump(
        {"flows": {"myflow": {"name": "myflow", "code_path": "code/pipelines/myflow"}}}))


def _launched_cmd(monkeypatch, root, **kwargs) -> str:
    """Run mcp_run with screen/subprocess stubbed; return the snakemake shell line."""
    import shutil
    import subprocess

    from pipeio.mcp import mcp_run

    calls = []
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: calls.append(cmd))
    result = mcp_run(root, flow="myflow", **kwargs)
    assert result.get("launched"), result
    launch = [c for c in calls if c[:2] == ["screen", "-dmS"]]
    return launch[-1][-1]


def test_mcp_run_injects_profiles_by_default(tmp_path, monkeypatch):
    root, flow_dir = _project(tmp_path)
    _registered(root)
    line = _launched_cmd(monkeypatch, root)
    assert f"--profile {root / '_profiles/base'}" in line
    assert f"--workflow-profile {flow_dir / 'profiles' / 'default'}" in line


def test_mcp_run_extra_profile_wins(tmp_path, monkeypatch):
    root, flow_dir = _project(tmp_path)
    _registered(root)
    line = _launched_cmd(monkeypatch, root, extra_args=["--profile", "/custom"])
    assert str(root / "_profiles/base") not in line
    assert "--profile /custom" in line


def test_mcp_run_empty_profile_args_disables(tmp_path, monkeypatch):
    root, _ = _project(tmp_path)
    _registered(root)
    line = _launched_cmd(monkeypatch, root, profile_args=[])
    assert "--profile" not in line and "--workflow-profile" not in line
