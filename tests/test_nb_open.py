"""Tests for ``pipeio nb open`` — resolution, dedupe, launch, completion.

No marimo installation required: the server registry is a fake directory and
the process/HTTP probes are injected.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from pipeio.notebook import marimo_server as ms
from pipeio.notebook.lifecycle import (
    list_flow_notebooks,
    nb_open,
    resolve_flow_notebook,
)


MARIMO_SOURCE = textwrap.dedent("""\
    import marimo

    app = marimo.App(width="medium")

    @app.cell
    def setup():
        x = 1
        return (x,)

    if __name__ == "__main__":
        app.run()
""")

PERCENT_SOURCE = textwrap.dedent("""\
    # %% [markdown]
    # # Title

    # %%
    x = 1
""")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A project root with one registered flow and three notebooks."""
    flow_dir = tmp_path / "code" / "pipelines" / "swr"
    (flow_dir / "notebooks" / "explore").mkdir(parents=True)
    (flow_dir / "notebooks" / "demo" / ".src").mkdir(parents=True)

    (flow_dir / "notebooks" / "explore" / "ripples.py").write_text(
        MARIMO_SOURCE, encoding="utf-8")
    (flow_dir / "notebooks" / "demo" / "ripples.py").write_text(
        MARIMO_SOURCE, encoding="utf-8")
    (flow_dir / "notebooks" / "demo" / ".src" / "summary.py").write_text(
        PERCENT_SOURCE, encoding="utf-8")

    reg_dir = tmp_path / ".projio" / "pipeio"
    reg_dir.mkdir(parents=True)
    (reg_dir / "registry.yml").write_text(yaml.safe_dump({
        "flows": {
            "swr": {"name": "swr", "code_path": "code/pipelines/swr"},
        },
    }), encoding="utf-8")
    return tmp_path


def _entry(port: int, pid: int = 4242, host: str = "0.0.0.0") -> dict:
    return {
        "server_id": f"{host}:{port}",
        "pid": pid,
        "host": host,
        "port": port,
        "base_url": "",
        "started_at": "2026-09-20T10:00:00+00:00",
        "version": "0.23.1",
    }


# ---------------------------------------------------------------------------
# Candidate listing
# ---------------------------------------------------------------------------

class TestListFlowNotebooks:

    def test_lists_both_workspaces(self, project):
        found = list_flow_notebooks(project, "swr")
        pairs = {(c["workspace"], c["name"]) for c in found}
        assert ("explore", "ripples") in pairs
        assert ("demo", "ripples") in pairs
        assert ("demo", "summary") in pairs  # .src/ layout

    def test_records_format(self, project):
        found = {(c["workspace"], c["name"]): c for c in list_flow_notebooks(project, "swr")}
        assert found[("explore", "ripples")]["format"] == "marimo"
        assert found[("demo", "summary")]["format"] == "percent"

    def test_unknown_flow_is_empty(self, project):
        assert list_flow_notebooks(project, "nope") == []

    def test_no_registry_is_empty(self, tmp_path):
        assert list_flow_notebooks(tmp_path, "swr") == []


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

class TestResolveFlowNotebook:

    def test_defaults_to_explore(self, project):
        result = resolve_flow_notebook(project, "swr", "ripples")
        assert result["workspace"] == "explore"
        assert result["path"].endswith("notebooks/explore/ripples.py")

    def test_demo_workspace(self, project):
        result = resolve_flow_notebook(project, "swr", "ripples", workspace="demo")
        assert result["workspace"] == "demo"
        assert result["path"].endswith("notebooks/demo/ripples.py")

    def test_accepts_py_suffix(self, project):
        with_suffix = resolve_flow_notebook(project, "swr", "ripples.py")
        without = resolve_flow_notebook(project, "swr", "ripples")
        assert with_suffix["path"] == without["path"]

    def test_accepts_workspace_prefix(self, project):
        result = resolve_flow_notebook(project, "swr", "demo/ripples.py")
        assert result["workspace"] == "demo"

    def test_auto_detects_lone_demo_notebook(self, project):
        """summary only exists in demo/ — no --demo needed."""
        result = resolve_flow_notebook(project, "swr", "summary")
        assert result["workspace"] == "demo"

    def test_unknown_flow_lists_flows(self, project):
        result = resolve_flow_notebook(project, "bogus", "ripples")
        assert "Unknown flow" in result["error"]
        assert result["candidates"] == ["swr"]
        assert result["candidate_kind"] == "flow"

    def test_unknown_notebook_lists_candidates(self, project):
        result = resolve_flow_notebook(project, "swr", "nope")
        assert "Unknown notebook" in result["error"]
        assert "explore/ripples" in result["candidates"]
        assert result["candidate_kind"] == "notebook"

    def test_wrong_workspace_is_an_error(self, project):
        result = resolve_flow_notebook(project, "swr", "summary", workspace="explore")
        assert "error" in result


# ---------------------------------------------------------------------------
# Server registry
# ---------------------------------------------------------------------------

class TestServerRegistry:

    def test_lists_live_entries_only(self, tmp_path):
        d = tmp_path / "servers"
        d.mkdir()
        (d / "a.json").write_text(json.dumps(_entry(2718, pid=111)), encoding="utf-8")
        (d / "b.json").write_text(json.dumps(_entry(2719, pid=222)), encoding="utf-8")

        live = ms.list_servers(directory=d, is_alive=lambda pid: pid == 111)
        assert [e["port"] for e in live] == [2718]

    def test_prune_removes_dead_entries(self, tmp_path):
        d = tmp_path / "servers"
        d.mkdir()
        dead = d / "b.json"
        dead.write_text(json.dumps(_entry(2719, pid=222)), encoding="utf-8")

        ms.list_servers(directory=d, prune=True, is_alive=lambda pid: False)
        assert not dead.exists()

    def test_ignores_malformed_json(self, tmp_path):
        d = tmp_path / "servers"
        d.mkdir()
        (d / "bad.json").write_text("{not json", encoding="utf-8")
        assert ms.list_servers(directory=d, is_alive=lambda pid: True) == []

    def test_missing_directory(self, tmp_path):
        assert ms.list_servers(directory=tmp_path / "nothing") == []

    def test_wildcard_host_renders_as_localhost(self):
        assert ms.server_url(_entry(2718)) == "http://localhost:2718"

    def test_explicit_host_kept(self):
        assert ms.server_url(_entry(2718, host="127.0.0.1")) == "http://127.0.0.1:2718"

    def test_base_url_appended(self):
        entry = {**_entry(2718), "base_url": "/lab"}
        assert ms.server_url(entry) == "http://localhost:2718/lab"


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------

class TestFindServerForNotebook:

    def test_matches_serving_server(self, tmp_path):
        nb = tmp_path / "ripples.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")

        hit = ms.find_server_for_notebook(
            nb,
            servers=[_entry(2718), _entry(2719, pid=99)],
            probe=lambda e: (
                [{"path": str(nb), "session_id": "s1", "source": "http"}]
                if e["port"] == 2719 else []
            ),
        )
        assert hit is not None
        assert hit["port"] == 2719
        assert hit["session_id"] == "s1"
        assert hit["url"] == "http://localhost:2719"

    def test_no_match_returns_none(self, tmp_path):
        nb = tmp_path / "ripples.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")
        other = tmp_path / "other.py"

        hit = ms.find_server_for_notebook(
            nb,
            servers=[_entry(2718)],
            probe=lambda e: [{"path": str(other), "session_id": "", "source": "argv"}],
        )
        assert hit is None

    def test_matches_relative_and_symlinked_paths(self, tmp_path):
        real = tmp_path / "real" / "ripples.py"
        real.parent.mkdir()
        real.write_text(MARIMO_SOURCE, encoding="utf-8")
        link = tmp_path / "link.py"
        link.symlink_to(real)

        hit = ms.find_server_for_notebook(
            link,
            servers=[_entry(2718)],
            probe=lambda e: [{"path": str(real), "session_id": "", "source": "argv"}],
        )
        assert hit is not None

    def test_http_probe_only_when_argv_misses(self, tmp_path):
        nb = tmp_path / "ripples.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")
        http_calls: list[int] = []

        def fake_http(entry, timeout=1.0):
            http_calls.append(entry["port"])
            return [{"path": str(nb), "session_id": "s9", "source": "http"}]

        with patch.object(ms, "notebooks_from_argv",
                          return_value=[{"path": str(nb), "session_id": "", "source": "argv"}]), \
                patch.object(ms, "notebooks_from_http", side_effect=fake_http):
            hit = ms.find_server_for_notebook(nb, servers=[_entry(2718)])
        assert hit["matched_by"] == "argv"
        assert http_calls == []

        with patch.object(ms, "notebooks_from_argv", return_value=[]), \
                patch.object(ms, "notebooks_from_http", side_effect=fake_http):
            hit = ms.find_server_for_notebook(nb, servers=[_entry(2718)])
        assert hit["matched_by"] == "http"
        assert hit["session_id"] == "s9"
        assert http_calls == [2718]

    def test_argv_probe_ignores_flags(self):
        argv = ["marimo", "edit", "--headless", "--no-token", "--host",
                "0.0.0.0", "/tmp/x/ripples.py"]
        with patch.object(ms, "proc_argv", return_value=argv), \
                patch.object(ms, "proc_cwd", return_value=Path("/tmp/x")):
            hits = ms.notebooks_from_argv(1234)
        assert [h["path"] for h in hits] == ["/tmp/x/ripples.py"]

    def test_argv_probe_resolves_relative_paths(self):
        argv = ["marimo", "edit", "notebooks/explore/ripples.py"]
        with patch.object(ms, "proc_argv", return_value=argv), \
                patch.object(ms, "proc_cwd", return_value=Path("/tmp/proj")):
            hits = ms.notebooks_from_argv(1234)
        assert hits[0]["path"] == "/tmp/proj/notebooks/explore/ripples.py"


# ---------------------------------------------------------------------------
# Command construction
# ---------------------------------------------------------------------------

class TestBuildCommand:

    def test_agent_discoverable_flag_bundle(self, tmp_path):
        cmd = ms.build_command(tmp_path / "nb.py", marimo_cmd=["marimo"])
        assert cmd[:2] == ["marimo", "edit"]
        assert "--headless" in cmd
        assert "--no-token" in cmd  # registry visibility for agents
        assert "--host" in cmd and "0.0.0.0" in cmd
        assert "--port" not in cmd  # auto-pick a free port
        assert cmd[-1] == str(tmp_path / "nb.py")

    def test_port_pins(self, tmp_path):
        cmd = ms.build_command(tmp_path / "nb.py", marimo_cmd=["marimo"], port=2750)
        assert cmd[cmd.index("--port") + 1] == "2750"

    def test_extra_args_appended(self, tmp_path):
        cmd = ms.build_command(
            tmp_path / "nb.py", marimo_cmd=["python", "-m", "marimo"],
            headless=False, extra_args=["--sandbox"],
        )
        assert "--headless" not in cmd
        assert cmd[-1] == "--sandbox"


# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

class _FakeProc:
    def __init__(self, pid: int = 4242, rc=None):
        self.pid = pid
        self.returncode = rc

    def poll(self):
        return self.returncode


class TestLaunch:

    def test_returns_url_once_registered(self, tmp_path):
        nb = tmp_path / "nb.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")

        with patch.object(ms.subprocess, "Popen", return_value=_FakeProc(4242)):
            result = ms.launch(
                nb, marimo_cmd=["marimo"], cwd=tmp_path,
                log_path=tmp_path / "log.txt",
                discover=lambda: [_entry(2718, pid=4242)],
            )
        assert result["status"] == "started"
        assert result["url"] == "http://localhost:2718"
        assert result["pid"] == 4242
        assert result["port"] == 2718

    def test_matches_grandchild_by_pinned_port(self, tmp_path):
        """`pixi run marimo` registers a different pid than Popen returns."""
        nb = tmp_path / "nb.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")

        with patch.object(ms.subprocess, "Popen", return_value=_FakeProc(4242)):
            result = ms.launch(
                nb, marimo_cmd=["pixi", "run", "marimo"], cwd=tmp_path, port=2750,
                discover=lambda: [_entry(2750, pid=9999)],
            )
        assert result["status"] == "started"
        assert result["port"] == 2750

    def test_reports_early_exit(self, tmp_path):
        nb = tmp_path / "nb.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")
        log = tmp_path / "log.txt"

        with patch.object(ms.subprocess, "Popen", return_value=_FakeProc(4242, rc=1)):
            result = ms.launch(
                nb, marimo_cmd=["marimo"], cwd=tmp_path, log_path=log,
                discover=lambda: [],
            )
        assert result["status"] == "error"
        assert result["exit_code"] == 1

    def test_missing_marimo_is_an_error(self, tmp_path):
        nb = tmp_path / "nb.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")

        with patch.object(ms.subprocess, "Popen", side_effect=FileNotFoundError):
            result = ms.launch(nb, marimo_cmd=["marimo"], cwd=tmp_path,
                               discover=lambda: [])
        assert result["status"] == "error"
        assert "not found" in result["error"]

    def test_unregistered_after_timeout(self, tmp_path):
        nb = tmp_path / "nb.py"
        nb.write_text(MARIMO_SOURCE, encoding="utf-8")

        with patch.object(ms.subprocess, "Popen", return_value=_FakeProc(4242)):
            result = ms.launch(
                nb, marimo_cmd=["marimo"], cwd=tmp_path,
                discover=lambda: [], timeout=0.0, poll=0.01,
            )
        assert result["status"] == "unregistered"


# ---------------------------------------------------------------------------
# nb_open (end to end, launch mocked)
# ---------------------------------------------------------------------------

class TestNbOpen:

    def test_reuses_existing_server(self, project):
        nb = project / "code" / "pipelines" / "swr" / "notebooks" / "explore" / "ripples.py"
        existing = {
            "server_id": "0.0.0.0:2718", "pid": 4242, "host": "0.0.0.0",
            "port": 2718, "url": "http://localhost:2718", "session_id": "s1",
            "notebook_path": str(nb),
        }
        with patch.object(ms, "find_server_for_notebook", return_value=existing) as finder, \
                patch.object(ms, "launch") as launcher:
            result = nb_open(project, "swr", "ripples")

        assert result["reused"] is True
        assert result["url"] == "http://localhost:2718"
        assert finder.called
        assert not launcher.called

    def test_force_launches_anyway(self, project):
        with patch.object(ms, "find_server_for_notebook") as finder, \
                patch.object(ms, "launch", return_value={
                    "status": "started", "pid": 1, "url": "http://localhost:2719",
                    "port": 2719, "host": "0.0.0.0", "server_id": "0.0.0.0:2719",
                }) as launcher:
            result = nb_open(project, "swr", "ripples", reuse=False)

        assert result["reused"] is False
        assert not finder.called
        assert launcher.called

    def test_passes_port_and_workspace(self, project):
        with patch.object(ms, "find_server_for_notebook", return_value=None), \
                patch.object(ms, "launch", return_value={"status": "started"}) as launcher:
            nb_open(project, "swr", "ripples", workspace="demo", port=2750)

        kwargs = launcher.call_args.kwargs
        assert kwargs["port"] == 2750
        assert str(launcher.call_args.args[0]).endswith("notebooks/demo/ripples.py")

    def test_log_path_defaults_under_projio(self, project):
        with patch.object(ms, "find_server_for_notebook", return_value=None), \
                patch.object(ms, "launch", return_value={"status": "started"}) as launcher:
            nb_open(project, "swr", "ripples")

        log = Path(launcher.call_args.kwargs["log_path"])
        assert log == project / ".projio" / "pipeio" / "logs" / "marimo-swr-ripples.log"

    def test_percent_notebook_is_rejected(self, project):
        result = nb_open(project, "swr", "summary")
        assert "error" in result
        assert "nb_lab" in result["error"]

    def test_unknown_notebook_returns_candidates(self, project):
        result = nb_open(project, "swr", "nope")
        assert "error" in result
        assert result["candidates"]

    def test_marimo_command_from_python_bin(self):
        from pipeio.notebook.lifecycle import marimo_command

        assert marimo_command("conda run -n cogpy python") == [
            "conda", "run", "-n", "cogpy", "python", "-m", "marimo",
        ]


# ---------------------------------------------------------------------------
# CLI wiring and completion
# ---------------------------------------------------------------------------

class TestCliOpen:

    def test_dispatches_to_nb_open(self, project):
        from pipeio import cli

        with patch("pipeio.notebook.lifecycle.nb_open", return_value={
            "status": "started", "reused": False, "url": "http://localhost:2718",
            "pid": 4242, "notebook_path": "nb.py", "log": "",
        }) as opener:
            rc = cli.main(["nb", "--root", str(project), "open", "swr", "ripples",
                           "--port", "2718", "--demo"])

        assert rc == 0
        kwargs = opener.call_args.kwargs
        assert kwargs["port"] == 2718
        assert kwargs["workspace"] == "demo"

    def test_extra_marimo_args_after_separator(self, project):
        from pipeio import cli

        with patch("pipeio.notebook.lifecycle.nb_open", return_value={
            "status": "started", "reused": False, "url": "u", "pid": 1,
            "notebook_path": "nb.py", "log": "",
        }) as opener:
            cli.main(["nb", "--root", str(project), "open", "swr", "ripples",
                      "--", "--sandbox"])

        assert opener.call_args.kwargs["extra_args"] == ["--sandbox"]

    def test_unknown_notebook_exits_nonzero(self, project, capsys):
        from pipeio import cli

        rc = cli.main(["nb", "--root", str(project), "open", "swr", "nope"])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Unknown notebook" in err
        assert "explore/ripples" in err

    def test_flow_completer(self, project):
        from pipeio import cli

        args = cli.argparse.Namespace(root=str(project))
        assert cli._flow_completer("", args) == ["swr"]
        assert cli._flow_completer("s", args) == ["swr"]
        assert cli._flow_completer("z", args) == []

    def test_notebook_completer_scoped_to_flow(self, project):
        from pipeio import cli

        args = cli.argparse.Namespace(root=str(project), flow="swr")
        assert cli._notebook_completer("", args) == ["ripples", "summary"]
        assert cli._notebook_completer("ri", args) == ["ripples"]

    def test_notebook_completer_without_flow(self, project):
        from pipeio import cli

        args = cli.argparse.Namespace(root=str(project), flow=None)
        assert cli._notebook_completer("", args) == []
