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
    reg_path.write_text(yaml.safe_dump({"flows": {"test": {"name": "test",
                "code_path": "code/pipelines/test",
            },
        },
    }))
    capsys.readouterr()
    ret = main(["flow", "--root", str(tmp_path), "list"])
    assert ret == 0
    out = capsys.readouterr().out
    assert "test" in out


def test_flow_new(tmp_path, capsys):
    ret = main(["flow", "--root", str(tmp_path), "new", "myflow"])
    assert ret == 0
    flow_dir = tmp_path / "pipelines" / "myflow"
    assert flow_dir.is_dir()
    assert (flow_dir / "config.yml").exists()
    assert (flow_dir / "Snakefile").exists()
    assert (flow_dir / "publish.yml").exists()
    assert (flow_dir / "rules").is_dir()
    assert (flow_dir / "notebooks" / "explore" / ".src").is_dir()
    assert (flow_dir / "notebooks" / "demo" / ".src").is_dir()
    # Config should have manifest keys
    cfg = (flow_dir / "config.yml").read_text()
    assert "output_manifest" in cfg
    assert "input_manifest" in cfg


def test_flow_new_augments_existing(tmp_path, capsys):
    """Running flow new on existing flow adds missing dirs without overwriting."""
    main(["flow", "--root", str(tmp_path), "new", "myflow"])
    capsys.readouterr()
    flow_dir = tmp_path / "pipelines" / "myflow"
    # Remove a dir that should be re-created
    import shutil
    shutil.rmtree(flow_dir / "rules")
    assert not (flow_dir / "rules").exists()
    # Re-run — should augment, not fail
    ret = main(["flow", "--root", str(tmp_path), "new", "myflow"])
    assert ret == 0
    assert (flow_dir / "rules").is_dir()
    # Existing files should not be overwritten
    assert (flow_dir / "Snakefile").exists()


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
