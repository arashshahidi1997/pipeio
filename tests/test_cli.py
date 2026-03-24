"""Tests for pipeio.cli."""

import yaml
import pytest

from pipeio.cli import main


def test_no_args(capsys):
    ret = main([])
    assert ret == 0
    assert "pipeio" in capsys.readouterr().out


def test_init(tmp_path, monkeypatch):
    ret = main(["init", "--root", str(tmp_path)])
    assert ret == 0
    assert (tmp_path / ".pipeio" / "registry.yml").exists()
    assert (tmp_path / ".pipeio" / "templates" / "flow").is_dir()


def test_init_idempotent(tmp_path, capsys):
    main(["init", "--root", str(tmp_path)])
    ret = main(["init", "--root", str(tmp_path)])
    assert ret == 0
    assert "already initialized" in capsys.readouterr().out


def test_flow_list_no_registry(tmp_path, capsys):
    ret = main(["flow", "--root", str(tmp_path), "list"])
    assert ret == 1


def test_flow_list_empty(tmp_path, capsys):
    main(["init", "--root", str(tmp_path)])
    capsys.readouterr()  # clear init output
    ret = main(["flow", "--root", str(tmp_path), "list"])
    assert ret == 0
    assert "No flows" in capsys.readouterr().out


def test_flow_list_with_flows(tmp_path, capsys):
    main(["init", "--root", str(tmp_path)])
    # Manually add a flow to the registry
    reg_path = tmp_path / ".pipeio" / "registry.yml"
    reg_path.write_text(yaml.safe_dump({
        "flows": {
            "test": {
                "name": "test",
                "pipe": "test",
                "code_path": "code/pipelines/test",
            },
        },
    }))
    capsys.readouterr()
    ret = main(["flow", "--root", str(tmp_path), "list"])
    assert ret == 0
    out = capsys.readouterr().out
    assert "test/test" in out


def test_flow_new(tmp_path, capsys):
    ret = main(["flow", "--root", str(tmp_path), "new", "mypipe", "myflow"])
    assert ret == 0
    # Check flow scaffold was created
    flow_dir = tmp_path / "pipelines" / "mypipe" / "myflow"
    assert flow_dir.is_dir()
    assert (flow_dir / "config.yml").exists()
    assert (flow_dir / "Snakefile").exists()


def test_flow_new_duplicate(tmp_path, capsys):
    main(["flow", "--root", str(tmp_path), "new", "mypipe", "myflow"])
    capsys.readouterr()
    ret = main(["flow", "--root", str(tmp_path), "new", "mypipe", "myflow"])
    assert ret == 1
    assert "already exists" in capsys.readouterr().err


def test_registry_scan(tmp_path, capsys):
    main(["init", "--root", str(tmp_path)])
    # Create a flow for scanning
    pipes_dir = tmp_path / "pipelines" / "test"
    pipes_dir.mkdir(parents=True)
    (pipes_dir / "Snakefile").touch()
    (pipes_dir / "config.yml").touch()

    capsys.readouterr()
    ret = main(["registry", "--root", str(tmp_path), "scan", "--pipelines-dir", str(tmp_path / "pipelines")])
    assert ret == 0
    out = capsys.readouterr().out
    assert "Written" in out

    # Verify the registry was updated
    from pipeio.registry import PipelineRegistry
    reg = PipelineRegistry.from_yaml(tmp_path / ".pipeio" / "registry.yml")
    assert len(reg.list_flows()) > 0


def test_registry_validate(tmp_path, capsys):
    main(["init", "--root", str(tmp_path)])
    capsys.readouterr()
    ret = main(["registry", "--root", str(tmp_path), "validate"])
    assert ret == 0
    assert "valid" in capsys.readouterr().out
