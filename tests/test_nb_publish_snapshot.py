"""nb_publish / nb_snapshot for both notebook backends, and the Snakefile preamble.

Marimo is not required: a fake ``python`` launcher stands in for the project
env (``python_bin``) and answers ``-m marimo export html|session``, so the real
subprocess path is exercised. One test runs real marimo when it is installed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

MARIMO_SOURCE = textwrap.dedent("""\
    import marimo

    app = marimo.App()

    @app.cell
    def _():
        x = 21 * 2
        print(x)
        return (x,)

    if __name__ == "__main__":
        app.run()
""")

PERCENT_SOURCE = "# %% [markdown]\n# # Title\n\n# %%\nx = 1\n"

FAKE_SESSION = {
    "version": "1",
    "cells": [
        {"id": "a", "console": [{"type": "stream", "name": "stdout", "text": "42\n"}],
         "outputs": [{"type": "data", "data": {"text/plain": "42"}}]},
        {"id": "b", "console": [],
         "outputs": [{"type": "error", "ename": "ValueError", "evalue": "boom"}]},
    ],
}


def _fake_python(tmp_path: Path, *, fail: bool = False) -> Path:
    """A launcher that mimics ``<python> -m marimo export {html,session} ...``."""
    script = tmp_path / "fake_python"
    session = json.dumps(FAKE_SESSION)
    script.write_text(textwrap.dedent(f"""\
        #!{sys.executable}
        import sys, pathlib
        args = sys.argv[1:]
        assert args[:2] == ["-m", "marimo"], args
        args = args[2:]
        if {fail!r}:
            sys.stderr.write("ModuleNotFoundError: No module named 'cogpy'\\n")
            sys.exit(1)
        if args[:2] == ["export", "html"]:
            out = pathlib.Path(args[args.index("-o") + 1])
            out.write_text("<html>marimo " + args[2] + "</html>")
        elif args[:2] == ["export", "session"]:
            nb = pathlib.Path(args[2])
            d = nb.parent / "__marimo__" / "session"
            d.mkdir(parents=True, exist_ok=True)
            (d / (nb.name + ".json")).write_text({session!r})
        else:
            sys.exit(2)
    """), encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """One flow with a marimo notebook and a paired percent notebook."""
    root = tmp_path / "proj"
    flow_dir = root / "code" / "pipelines" / "swr"
    nb_dir = flow_dir / "notebooks"
    (nb_dir / "explore").mkdir(parents=True)
    (nb_dir / "demo" / ".src").mkdir(parents=True)
    (nb_dir / "demo" / ".myst").mkdir(parents=True)

    (nb_dir / "explore" / "ripples.py").write_text(MARIMO_SOURCE, encoding="utf-8")
    (nb_dir / "demo" / ".src" / "summary.py").write_text(PERCENT_SOURCE, encoding="utf-8")
    (nb_dir / "demo" / "summary.ipynb").write_text("{}", encoding="utf-8")
    (nb_dir / "demo" / ".myst" / "summary.md").write_text("# Summary\n", encoding="utf-8")

    (nb_dir / "notebook.yml").write_text(yaml.safe_dump({
        "entries": [
            {"path": "notebooks/explore/ripples.py", "kind": "investigate",
             "format": "marimo", "publish_html": True},
            {"path": "notebooks/demo/.src/summary.py", "kind": "demo",
             "publish_html": True, "publish_myst": True},
        ],
    }), encoding="utf-8")

    reg_dir = root / ".projio" / "pipeio"
    reg_dir.mkdir(parents=True)
    (reg_dir / "registry.yml").write_text(yaml.safe_dump({
        "flows": {"swr": {"name": "swr", "code_path": "code/pipelines/swr"}},
    }), encoding="utf-8")
    return root


@pytest.fixture
def no_marimo_in_env():
    """The MCP server env has no marimo: backend must still register."""
    from pipeio.notebook import backend as b

    b._BACKENDS.clear()
    real_run = __import__("subprocess").run

    def run(cmd, *a, **kw):  # only the `--version` probe sees "no marimo"
        if cmd[-1] == "--version":
            raise FileNotFoundError("marimo")
        return real_run(cmd, *a, **kw)

    with patch("pipeio.notebook.backend_marimo.subprocess.run", side_effect=run):
        yield
    b._BACKENDS.clear()


# ---------------------------------------------------------------------------
# Backend registration
# ---------------------------------------------------------------------------

def test_marimo_backend_registers_without_marimo(no_marimo_in_env, project):
    from pipeio.notebook.backend import detect_format, get_backend, list_backends

    assert "marimo" in list_backends()
    nb = project / "code/pipelines/swr/notebooks/explore/ripples.py"
    assert detect_format(nb) == "marimo"
    assert get_backend("marimo")._marimo_cmd == ["marimo"]


# ---------------------------------------------------------------------------
# nb_publish
# ---------------------------------------------------------------------------

def test_publish_marimo_exports_html(no_marimo_in_env, project, tmp_path):
    from pipeio.mcp import mcp_nb_publish

    res = mcp_nb_publish(project, "swr", "ripples", python_bin=str(_fake_python(tmp_path)))
    assert "error" not in res, res
    assert res["format"] == "marimo"
    out = project / "code/pipelines/swr/.build/notebooks/ripples.html"
    assert res["published"] == [str(out)]
    assert out.read_text().startswith("<html>marimo")
    assert "warning" not in res


def test_publish_marimo_autodetects_without_format_field(no_marimo_in_env, project, tmp_path):
    from pipeio.mcp import mcp_nb_publish

    cfg = project / "code/pipelines/swr/notebooks/notebook.yml"
    data = yaml.safe_load(cfg.read_text())
    data["entries"][0].pop("format")
    data["entries"][0]["publish_html"] = False
    cfg.write_text(yaml.safe_dump(data))

    res = mcp_nb_publish(project, "swr", "ripples", format="html",
                         python_bin=str(_fake_python(tmp_path)))
    assert res["format"] == "marimo"
    assert "publish_html" in res["warning"]


def test_publish_marimo_myst_is_refused(no_marimo_in_env, project, tmp_path):
    from pipeio.mcp import mcp_nb_publish

    res = mcp_nb_publish(project, "swr", "ripples", format="myst",
                         python_bin=str(_fake_python(tmp_path)))
    assert "percent-only" in res["error"]


def test_publish_marimo_export_failure_reports_stderr(no_marimo_in_env, project, tmp_path):
    from pipeio.mcp import mcp_nb_publish

    res = mcp_nb_publish(project, "swr", "ripples",
                         python_bin=str(_fake_python(tmp_path, fail=True)))
    assert "cogpy" in res["error"]
    assert "python_bin" in res["hint"]


def test_publish_percent_html_and_myst(project):
    from pipeio.mcp import mcp_nb_publish

    def fake_convert(ipynb, dest):
        Path(dest).write_text("<html>nbconvert</html>")

    with patch("pipeio.notebook.lifecycle._nbconvert_html", side_effect=fake_convert):
        res = mcp_nb_publish(project, "swr", "summary")
    assert "error" not in res, res
    names = sorted(Path(p).name for p in res["published"])
    assert names == ["summary.html", "summary.md"]


def test_docs_collect_picks_up_marimo_html(no_marimo_in_env, project, tmp_path):
    from pipeio.docs import CollectContext, PublishConfig, export_notebooks
    from pipeio.mcp import mcp_nb_publish

    flow_dir = project / "code/pipelines/swr"
    ctx = CollectContext(
        entry=argparse.Namespace(name="swr"), flow_dir=flow_dir,
        target=project / "docs/pipelines/swr", root=project, publish=PublishConfig(),
    )
    with patch("pipeio.docs._nbconvert_html"):
        assert not any(p.endswith("ripples.html") for p in export_notebooks(ctx))
        mcp_nb_publish(project, "swr", "ripples", python_bin=str(_fake_python(tmp_path)))
        exported = export_notebooks(ctx)
    assert any(p.endswith("ripples.html") for p in exported)
    index = (flow_dir / ".build/notebooks/index.md").read_text()
    assert "ripples" in index


# ---------------------------------------------------------------------------
# nb_snapshot
# ---------------------------------------------------------------------------

def test_snapshot_marimo(no_marimo_in_env, project, tmp_path):
    from pipeio.mcp import mcp_nb_snapshot

    res = mcp_nb_snapshot(project, "swr", "ripples", python_bin=str(_fake_python(tmp_path)))
    assert "error" not in res, res
    assert res["cell_count"] == 2
    assert res["error_count"] == 1
    assert res["cells"][0]["console"].strip() == "42"


def test_snapshot_ignores_stale_session_json(no_marimo_in_env, project, tmp_path):
    from pipeio.mcp import mcp_nb_snapshot

    nb = project / "code/pipelines/swr/notebooks/explore/ripples.py"
    stale = nb.parent / "__marimo__" / "session" / "ripples.py.json"
    stale.parent.mkdir(parents=True)
    stale.write_text(json.dumps(FAKE_SESSION))
    os.utime(stale, (1, 1))

    res = mcp_nb_snapshot(project, "swr", "ripples",
                          python_bin=str(_fake_python(tmp_path, fail=True)))
    assert "not found" in res["error"]
    assert "cogpy" in res["stderr"]


def test_snapshot_percent_is_refused(project):
    from pipeio.mcp import mcp_nb_snapshot

    res = mcp_nb_snapshot(project, "swr", "summary")
    assert "only supported for marimo" in res["error"]


@pytest.mark.skipif(
    not (shutil.which("marimo") or __import__("importlib").util.find_spec("marimo")),
    reason="marimo not installed",
)
def test_real_marimo_publish_and_snapshot(project):
    from pipeio.notebook import backend as b
    from pipeio.mcp import mcp_nb_publish, mcp_nb_snapshot

    b._BACKENDS.clear()
    res = mcp_nb_publish(project, "swr", "ripples")
    assert "error" not in res, res
    snap = mcp_nb_snapshot(project, "swr", "ripples")
    assert "error" not in snap, snap
    assert any("42" in c.get("console", "") for c in snap["cells"])


# ---------------------------------------------------------------------------
# Snakefile preamble / includes from project config
# ---------------------------------------------------------------------------

def _write_scaffold_cfg(root: Path, scaffold: dict) -> None:
    d = root / ".projio" / "pipeio"
    d.mkdir(parents=True, exist_ok=True)
    (d / "config.yml").write_text(yaml.safe_dump({"scaffold": scaffold}))


def test_snakefile_default_has_no_includes(tmp_path):
    from pipeio.mcp import _flow_snakefile

    text = _flow_snakefile("swr", tmp_path)
    assert 'configfile: "config.yml"' in text
    assert "include:" not in text
    assert text.rstrip().endswith("input: []")


def test_flow_new_mcp_writes_includes_and_preamble(tmp_path):
    from pipeio.mcp import mcp_flow_new

    (tmp_path / ".projio").mkdir()
    _write_scaffold_cfg(tmp_path, {
        "snakefile_includes": ["../_common/provenance.smk"],
        "snakefile_preamble": "from mylib.flow import setup\n",
    })
    res = mcp_flow_new(tmp_path, "swr")
    assert "error" not in res, res
    lines = (tmp_path / "code/pipelines/swr/Snakefile").read_text().splitlines()
    i = lines.index('configfile: "config.yml"')
    assert lines[i + 1] == 'include: "../_common/provenance.smk"'
    assert "from mylib.flow import setup" in lines
    assert lines.index("from mylib.flow import setup") < lines.index("rule all:")


def test_flow_new_cli_uses_same_template(tmp_path):
    from pipeio.cli import _cmd_flow_new
    from pipeio.mcp import _flow_snakefile

    (tmp_path / "code" / "pipelines").mkdir(parents=True)
    _write_scaffold_cfg(tmp_path, {"snakefile_includes": "../_common/provenance.smk"})
    _cmd_flow_new(argparse.Namespace(root=str(tmp_path), flow="swr"))
    written = (tmp_path / "code/pipelines/swr/Snakefile").read_text()
    assert written == _flow_snakefile("swr", tmp_path)
    assert 'include: "../_common/provenance.smk"' in written
