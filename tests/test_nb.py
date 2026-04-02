"""Tests for pipeio notebook lifecycle."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from pipeio.notebook.lifecycle import find_notebook_configs, nb_status


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_nb_config(
    flow_root: Path,
    *,
    pair_ipynb: bool = True,
    pair_myst: bool = False,
    publish_html: bool = False,
    publish_myst: bool = False,
    docs_dir: str = "") -> None:
    """Write a notebooks/notebook.yml under flow_root."""
    nb_dir = flow_root / "notebooks"
    nb_dir.mkdir(parents=True, exist_ok=True)
    cfg = {"publish": {"docs_dir": docs_dir, "prefix": "nb-"},
        "entries": [
            {"path": "notebooks/analysis.py",
                "pair_ipynb": pair_ipynb,
                "pair_myst": pair_myst,
                "publish_html": publish_html,
                "publish_myst": publish_myst,
            }
        ],
    }
    (nb_dir / "notebook.yml").write_text(yaml.safe_dump(cfg), encoding="utf-8")


def _make_py(flow_root: Path) -> Path:
    py = flow_root / "notebooks" / "analysis.py"
    py.write_text("# %% [markdown]\n# Analysis\n# %%\nx = 1\n", encoding="utf-8")
    return py


def _make_ipynb(flow_root: Path, *, with_outputs: bool = False) -> Path:
    ipynb = flow_root / "notebooks" / "analysis.ipynb"
    nb = {"nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"kernelspec": {"name": "python3"}},
        "cells": [
            {"cell_type": "code",
                "source": "x = 1",
                "metadata": {},
                "outputs": [{"output_type": "execute_result", "data": {"text/plain": "1"}}] if with_outputs else [],
                "execution_count": 1 if with_outputs else None,
            }
        ],
    }
    ipynb.write_text(json.dumps(nb), encoding="utf-8")
    return ipynb


# ---------------------------------------------------------------------------
# find_notebook_configs
# ---------------------------------------------------------------------------

def test_find_no_configs(tmp_path):
    assert find_notebook_configs(tmp_path) == []


def test_find_one_config(tmp_path):
    _make_nb_config(tmp_path)
    results = find_notebook_configs(tmp_path)
    assert len(results) == 1
    flow_root, cfg = results[0]
    assert flow_root == tmp_path
    assert len(cfg.entries) == 1


def test_find_multiple_flows(tmp_path):
    for name in ("flow_a", "flow_b"):
        _make_nb_config(tmp_path / name)
    results = find_notebook_configs(tmp_path)
    assert len(results) == 2


# ---------------------------------------------------------------------------
# nb_status
# ---------------------------------------------------------------------------

def test_nb_status_no_notebooks(tmp_path):
    assert nb_status(tmp_path) == []


def test_nb_status_py_missing(tmp_path):
    _make_nb_config(tmp_path)
    statuses = nb_status(tmp_path)
    assert len(statuses) == 1
    s = statuses[0]
    assert s["py_exists"] is False
    assert s["synced"] is False


def test_nb_status_py_only(tmp_path):
    _make_nb_config(tmp_path)
    _make_py(tmp_path)
    statuses = nb_status(tmp_path)
    s = statuses[0]
    assert s["py_exists"] is True
    assert s["ipynb_exists"] is False
    assert s["synced"] is False
    assert s["executed"] is False


def test_nb_status_synced_not_executed(tmp_path):
    _make_nb_config(tmp_path)
    py = _make_py(tmp_path)
    ipynb = _make_ipynb(tmp_path, with_outputs=False)
    # Make ipynb newer than py
    import os
    future = py.stat().st_mtime + 10
    os.utime(ipynb, (future, future))

    statuses = nb_status(tmp_path)
    s = statuses[0]
    assert s["synced"] is True
    assert s["executed"] is False


def test_nb_status_executed(tmp_path):
    _make_nb_config(tmp_path)
    py = _make_py(tmp_path)
    ipynb = _make_ipynb(tmp_path, with_outputs=True)
    import os
    future = py.stat().st_mtime + 10
    os.utime(ipynb, (future, future))

    statuses = nb_status(tmp_path)
    s = statuses[0]
    assert s["executed"] is True


def test_nb_status_name(tmp_path):
    _make_nb_config(tmp_path)
    statuses = nb_status(tmp_path)
    assert statuses[0]["name"] == "analysis"


def test_nb_status_myst_none_when_not_configured(tmp_path):
    _make_nb_config(tmp_path, pair_myst=False)
    _make_py(tmp_path)
    statuses = nb_status(tmp_path)
    assert statuses[0]["myst_exists"] is None


# ---------------------------------------------------------------------------
# nb_pair (mocked subprocess)
# ---------------------------------------------------------------------------

def test_nb_pair_creates_ipynb(tmp_path):
    _make_nb_config(tmp_path)
    _make_py(tmp_path)

    calls = []

    def fake_run(cmd, check):
        calls.append(cmd)
        # Simulate jupytext creating the output file
        out_idx = cmd.index("--output") + 1
        Path(cmd[out_idx]).touch()

    with patch("pipeio.notebook.lifecycle._require_jupytext"):
        with patch("pipeio.notebook.lifecycle.subprocess.run", side_effect=fake_run):
            from pipeio.notebook.lifecycle import nb_pair
            created = nb_pair(tmp_path)

    assert len(created) == 1
    assert created[0].endswith(".ipynb")
    assert len(calls) == 1
    assert "--to" in calls[0]
    assert "notebook" in calls[0]


def test_nb_pair_skips_existing(tmp_path):
    _make_nb_config(tmp_path)
    _make_py(tmp_path)
    _make_ipynb(tmp_path)  # already exists

    calls = []
    with patch("pipeio.notebook.lifecycle._require_jupytext"):
        with patch("pipeio.notebook.lifecycle.subprocess.run", side_effect=lambda cmd, check: calls.append(cmd)):
            from pipeio.notebook.lifecycle import nb_pair
            created = nb_pair(tmp_path)

    assert created == []
    assert calls == []


def test_nb_pair_force_recreates(tmp_path):
    _make_nb_config(tmp_path)
    _make_py(tmp_path)
    _make_ipynb(tmp_path)  # already exists

    calls = []

    def fake_run(cmd, check):
        calls.append(cmd)
        Path(cmd[cmd.index("--output") + 1]).touch()

    with patch("pipeio.notebook.lifecycle._require_jupytext"):
        with patch("pipeio.notebook.lifecycle.subprocess.run", side_effect=fake_run):
            from pipeio.notebook.lifecycle import nb_pair
            created = nb_pair(tmp_path, force=True)

    assert len(created) == 1
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# nb_sync (mocked subprocess)
# ---------------------------------------------------------------------------

def test_nb_sync_updates_stale(tmp_path):
    _make_nb_config(tmp_path)
    py = _make_py(tmp_path)
    ipynb = _make_ipynb(tmp_path)

    # Make ipynb older than py
    import os, time
    past = py.stat().st_mtime - 10
    os.utime(ipynb, (past, past))

    calls = []

    def fake_run(cmd, check):
        calls.append(cmd)
        Path(cmd[cmd.index("--output") + 1]).touch()

    with patch("pipeio.notebook.lifecycle._require_jupytext"):
        with patch("pipeio.notebook.lifecycle.subprocess.run", side_effect=fake_run):
            from pipeio.notebook.lifecycle import nb_sync
            updated = nb_sync(tmp_path)

    assert len(updated) == 1
    assert len(calls) == 1


def test_nb_sync_skips_fresh(tmp_path):
    _make_nb_config(tmp_path)
    py = _make_py(tmp_path)
    ipynb = _make_ipynb(tmp_path)

    import os
    future = py.stat().st_mtime + 10
    os.utime(ipynb, (future, future))

    calls = []
    with patch("pipeio.notebook.lifecycle._require_jupytext"):
        with patch("pipeio.notebook.lifecycle.subprocess.run", side_effect=lambda cmd, check: calls.append(cmd)):
            from pipeio.notebook.lifecycle import nb_sync
            updated = nb_sync(tmp_path)

    assert updated == []
    assert calls == []


# ---------------------------------------------------------------------------
# _nb_output_paths with workspace directories
# ---------------------------------------------------------------------------

def test_nb_output_paths_workspace_explore(tmp_path):
    from pipeio.notebook.lifecycle import _nb_output_paths
    py = tmp_path / "notebooks" / "explore" / ".src" / "investigate_noise.py"
    py.parent.mkdir(parents=True)
    py.touch()
    ipynb, myst = _nb_output_paths(py)
    assert ipynb == tmp_path / "notebooks" / "explore" / "investigate_noise.ipynb"
    assert myst == tmp_path / "notebooks" / "explore" / ".myst" / "investigate_noise.md"


def test_nb_output_paths_workspace_demo(tmp_path):
    from pipeio.notebook.lifecycle import _nb_output_paths
    py = tmp_path / "notebooks" / "demo" / ".src" / "demo_filter.py"
    py.parent.mkdir(parents=True)
    py.touch()
    ipynb, myst = _nb_output_paths(py)
    assert ipynb == tmp_path / "notebooks" / "demo" / "demo_filter.ipynb"
    assert myst == tmp_path / "notebooks" / "demo" / ".myst" / "demo_filter.md"


def test_nb_output_paths_flat(tmp_path):
    from pipeio.notebook.lifecycle import _nb_output_paths
    py = tmp_path / "notebooks" / ".src" / "analysis.py"
    py.parent.mkdir(parents=True)
    py.touch()
    ipynb, myst = _nb_output_paths(py)
    assert ipynb == tmp_path / "notebooks" / "analysis.ipynb"
    assert myst == tmp_path / "notebooks" / ".myst" / "analysis.md"


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

def test_cli_nb_status_no_notebooks(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["nb", "--root", str(tmp_path), "status"])
    assert ret == 0
    assert "No notebooks" in capsys.readouterr().out


def test_cli_nb_status_with_notebook(tmp_path, capsys):
    _make_nb_config(tmp_path)
    _make_py(tmp_path)
    from pipeio.cli import main
    ret = main(["nb", "--root", str(tmp_path), "status"])
    assert ret == 0
    assert "analysis" in capsys.readouterr().out


def test_cli_nb_pair_missing_jupytext(tmp_path, capsys):
    _make_nb_config(tmp_path)
    _make_py(tmp_path)
    with patch("pipeio.notebook.lifecycle._require_jupytext", side_effect=ImportError("no jupytext")):
        from pipeio.cli import main
        ret = main(["nb", "--root", str(tmp_path), "pair"])
    assert ret == 1
    assert "jupytext" in capsys.readouterr().err


def test_cli_nb_no_subcommand(tmp_path, capsys):
    from pipeio.cli import main
    ret = main(["nb", "--root", str(tmp_path)])
    assert ret == 0
