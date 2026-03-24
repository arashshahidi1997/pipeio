"""Tests for BidsResolver adapter.

These tests mock the snakebids import since it may not be installed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest
import yaml


@pytest.fixture(autouse=True)
def _mock_snakebids():
    """Ensure snakebids is importable (as a mock) for tests."""
    fake = ModuleType("snakebids")
    with patch.dict(sys.modules, {"snakebids": fake}):
        yield


def _make_config(tmp_path: Path) -> Path:
    cfg = {
        "output_dir": "results",
        "registry": {
            "deriv_preproc": {
                "bids": {
                    "root": "derivatives/preproc",
                    "datatype": "ieeg",
                },
                "members": {
                    "cleaned": {
                        "suffix": "cleaned",
                        "extension": ".edf",
                    },
                    "report": {
                        "suffix": "report",
                        "extension": ".html",
                    },
                },
            },
        },
    }
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return cfg_path


def test_resolve_basic(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)
    resolver = BidsResolver(cfg_path)
    path = resolver.resolve("deriv_preproc", "cleaned", sub="01", ses="pre")

    assert "sub-01" in str(path)
    assert "ses-pre" in str(path)
    assert "ieeg" in str(path)
    assert str(path).endswith(".edf")
    assert "suffix-cleaned" in str(path)


def test_resolve_sub_only(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)
    resolver = BidsResolver(cfg_path)
    path = resolver.resolve("deriv_preproc", "cleaned", sub="02")

    assert "sub-02" in str(path)
    assert "ses-" not in str(path)


def test_resolve_with_extra_entities(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)
    resolver = BidsResolver(cfg_path)
    path = resolver.resolve("deriv_preproc", "cleaned", sub="01", ses="pre", task="rest")

    parts = str(path)
    assert "sub-01" in parts
    assert "ses-pre" in parts
    assert "task-rest" in parts


def test_resolve_unknown_group(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)
    resolver = BidsResolver(cfg_path)
    with pytest.raises(KeyError, match="Unknown group"):
        resolver.resolve("nonexistent", "cleaned", sub="01")


def test_resolve_unknown_member(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)
    resolver = BidsResolver(cfg_path)
    with pytest.raises(KeyError, match="Unknown member"):
        resolver.resolve("deriv_preproc", "nonexistent", sub="01")


def test_expand_no_dir(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)
    resolver = BidsResolver(cfg_path)
    assert resolver.expand("deriv_preproc", "cleaned") == []


def test_expand_finds_files(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)

    # Create matching files on disk
    base = tmp_path / "results" / "derivatives" / "preproc" / "sub-01" / "ses-pre" / "ieeg"
    base.mkdir(parents=True)
    (base / "sub-01_ses-pre_suffix-cleaned.edf").touch()
    (base / "sub-01_ses-pre_suffix-report.html").touch()

    import os
    os.chdir(tmp_path)

    resolver = BidsResolver(cfg_path)
    matches = resolver.expand("deriv_preproc", "cleaned")
    assert len(matches) == 1
    assert "suffix-cleaned" in str(matches[0])


def test_expand_filters(tmp_path):
    from pipeio.adapters.bids import BidsResolver

    cfg_path = _make_config(tmp_path)

    base = tmp_path / "results" / "derivatives" / "preproc"
    for sub in ("sub-01", "sub-02"):
        d = base / sub / "ieeg"
        d.mkdir(parents=True)
        (d / f"{sub}_suffix-cleaned.edf").touch()

    import os
    os.chdir(tmp_path)

    resolver = BidsResolver(cfg_path)
    all_matches = resolver.expand("deriv_preproc", "cleaned")
    assert len(all_matches) == 2

    filtered = resolver.expand("deriv_preproc", "cleaned", sub="01")
    assert len(filtered) == 1
    assert "sub-01" in str(filtered[0])


def test_import_error_without_snakebids():
    """Without snakebids installed, BidsResolver should raise ImportError."""
    with patch.dict(sys.modules, {"snakebids": None}):
        # Force re-import
        import importlib
        from pipeio.adapters import bids
        importlib.reload(bids)
        with pytest.raises(ImportError, match="snakebids"):
            bids.BidsResolver(Path("config.yml"))
