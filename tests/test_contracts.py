"""Tests for contracts validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipeio.contracts import Contract, Check, ContractResult, validate_flow_contracts


# ---------------------------------------------------------------------------
# Basic Contract/Check
# ---------------------------------------------------------------------------

def test_contract_all_pass():
    contract = Contract(
        name="test",
        checks=[Check("exists", "file exists", lambda p: p.exists())],
    )
    import tempfile
    with tempfile.NamedTemporaryFile() as f:
        result = contract.validate([Path(f.name)])
    assert result.ok
    assert len(result.passed) == 1


def test_contract_failure(tmp_path):
    contract = Contract(
        name="test",
        checks=[Check("exists", "file exists", lambda p: p.exists())],
    )
    result = contract.validate([tmp_path / "nonexistent.txt"])
    assert not result.ok
    assert len(result.failed) == 1


def test_contract_error():
    def bad_check(p: Path) -> bool:
        raise RuntimeError("boom")

    contract = Contract(
        name="test",
        checks=[Check("bad", "always errors", bad_check)],
    )
    result = contract.validate([Path("x")])
    assert not result.ok
    assert len(result.errors) == 1
    assert "boom" in result.errors[0]


# ---------------------------------------------------------------------------
# validate_flow_contracts
# ---------------------------------------------------------------------------

def _scaffold(tmp_path: Path, *, with_config: bool = True, valid_config: bool = True) -> None:
    """Create a minimal project with registry and optional flow config."""
    pipeio_dir = tmp_path / ".pipeio"
    pipeio_dir.mkdir()

    flow_dir = tmp_path / "code" / "pipelines" / "preproc"
    flow_dir.mkdir(parents=True)

    config_path = str(flow_dir / "config.yml") if with_config else None

    reg = {
        "flows": {
            "preproc": {
                "name": "preproc",
                "pipe": "preproc",
                "code_path": "code/pipelines/preproc",
                "config_path": config_path,
            }
        }
    }
    (pipeio_dir / "registry.yml").write_text(yaml.safe_dump(reg), encoding="utf-8")

    if with_config:
        cfg: dict = {
            "input_dir": "sourcedata",
            "output_dir": "results",
            "registry": {},
        }
        if valid_config:
            cfg["registry"] = {
                "deriv": {
                    "members": {
                        "cleaned": {"suffix": "cleaned", "extension": ".edf"}
                    }
                }
            }
        (flow_dir / "config.yml").write_text(yaml.safe_dump(cfg), encoding="utf-8")


def test_validate_no_registry(tmp_path):
    results = validate_flow_contracts(tmp_path)
    assert results == []


def test_validate_no_config(tmp_path):
    _scaffold(tmp_path, with_config=False)
    results = validate_flow_contracts(tmp_path)
    assert len(results) == 1
    assert results[0].warnings  # should warn about missing config


def test_validate_valid_flow(tmp_path):
    _scaffold(tmp_path)
    # Create the input/output dirs
    (tmp_path / "sourcedata").mkdir()
    (tmp_path / "results").mkdir()

    results = validate_flow_contracts(tmp_path)
    assert len(results) == 1
    fv = results[0]
    assert fv.ok
    assert any("input_dir exists" in p for p in fv.passed)
    assert any("output_dir exists" in p for p in fv.passed)


def test_validate_missing_dirs(tmp_path):
    _scaffold(tmp_path)
    # Don't create input/output dirs
    results = validate_flow_contracts(tmp_path)
    fv = results[0]
    # Should warn about missing dirs (not error)
    assert any("input_dir not found" in w for w in fv.warnings)
    assert any("output_dir not found" in w for w in fv.warnings)


def test_validate_empty_group(tmp_path):
    _scaffold(tmp_path, valid_config=False)
    results = validate_flow_contracts(tmp_path)
    fv = results[0]
    # validate_config() warns about empty input/output dirs
    assert len(fv.warnings) > 0


def test_validate_projio_registry(tmp_path):
    """Should also find registry under .projio/pipeio/."""
    (tmp_path / ".projio" / "pipeio").mkdir(parents=True)
    flow_dir = tmp_path / "code" / "pipelines" / "preproc"
    flow_dir.mkdir(parents=True)

    reg = {
        "flows": {
            "preproc": {
                "name": "preproc",
                "pipe": "preproc",
                "code_path": "code/pipelines/preproc",
                "config_path": str(flow_dir / "config.yml"),
            }
        }
    }
    (tmp_path / ".projio" / "pipeio" / "registry.yml").write_text(
        yaml.safe_dump(reg), encoding="utf-8"
    )

    cfg = {"input_dir": "src", "output_dir": "out", "registry": {}}
    (flow_dir / "config.yml").write_text(yaml.safe_dump(cfg), encoding="utf-8")

    results = validate_flow_contracts(tmp_path)
    assert len(results) == 1


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

def test_cli_contracts_validate_no_registry(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["contracts", "--root", str(tmp_path), "validate"])
    assert ret == 0
    assert "No registry" in capsys.readouterr().out


def test_cli_contracts_validate_with_flow(tmp_path, capsys):
    _scaffold(tmp_path)
    (tmp_path / "sourcedata").mkdir()
    (tmp_path / "results").mkdir()
    from pipeio.cli import main
    ret = main(["contracts", "--root", str(tmp_path), "validate"])
    assert ret == 0
    out = capsys.readouterr().out
    assert "[OK]" in out
    assert "preproc/preproc" in out


def test_cli_contracts_no_subcommand(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["contracts", "--root", str(tmp_path)])
    assert ret == 0
