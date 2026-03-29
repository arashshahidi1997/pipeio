"""Tests for pipeio.notebook.analyze — static notebook analysis."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeio.notebook.analyze import (
    _build_cogpy_names,
    _extract_cogpy_calls,
    _extract_imports,
    _extract_pipeline_context,
    _extract_run_card,
    _extract_sections,
    _split_cells,
    analyze_notebook,
)
import ast


# ---------------------------------------------------------------------------
# _split_cells
# ---------------------------------------------------------------------------

def test_split_cells_empty():
    cells = _split_cells("")
    assert cells == [("code", "")]


def test_split_cells_no_markers():
    src = "import os\nx = 1\n"
    cells = _split_cells(src)
    assert len(cells) == 1
    assert cells[0][0] == "code"
    assert "import os" in cells[0][1]


def test_split_cells_markdown_and_code():
    src = (
        "# %% [markdown]\n"
        "# # Title\n"
        "#\n"
        "# %%\n"
        "import os\n"
    )
    cells = _split_cells(src)
    assert len(cells) == 2
    assert cells[0][0] == "markdown"
    assert "# Title" in cells[0][1]
    assert cells[1][0] == "code"
    assert "import os" in cells[1][1]


def test_split_cells_multiple_code():
    src = (
        "# %%\n"
        "x = 1\n"
        "# %%\n"
        "y = 2\n"
    )
    cells = _split_cells(src)
    assert len(cells) == 2
    assert all(k == "code" for k, _ in cells)
    assert "x = 1" in cells[0][1]
    assert "y = 2" in cells[1][1]


# ---------------------------------------------------------------------------
# _extract_sections
# ---------------------------------------------------------------------------

def test_extract_sections_h1():
    content = "# # My Title\n#\n# some text\n"
    assert _extract_sections(content) == ["# My Title"]


def test_extract_sections_h2():
    content = "# ## Setup\n# some text\n"
    assert _extract_sections(content) == ["## Setup"]


def test_extract_sections_multiple():
    content = "# ## Setup\n# %%\n# ## Analysis\n"
    headers = _extract_sections(content)
    assert "## Setup" in headers
    assert "## Analysis" in headers


def test_extract_sections_no_headers():
    content = "# just text\n# more text\n"
    assert _extract_sections(content) == []


# ---------------------------------------------------------------------------
# _extract_imports
# ---------------------------------------------------------------------------

def _parse(src: str) -> ast.Module:
    return ast.parse(src)


def test_extract_imports_plain():
    tree = _parse("import os\nimport sys\n")
    imports = _extract_imports(tree)
    modules = [i["module"] for i in imports]
    assert "os" in modules
    assert "sys" in modules


def test_extract_imports_from():
    tree = _parse("from cogpy.detect import ripple\n")
    imports = _extract_imports(tree)
    assert len(imports) == 1
    imp = imports[0]
    assert imp["kind"] == "from"
    assert imp["module"] == "cogpy.detect"
    assert imp["names"][0]["name"] == "ripple"


def test_extract_imports_alias():
    tree = _parse("import cogpy.brainstates as bs\n")
    imports = _extract_imports(tree)
    assert imports[0]["alias"] == "bs"
    assert imports[0]["module"] == "cogpy.brainstates"


# ---------------------------------------------------------------------------
# _extract_run_card
# ---------------------------------------------------------------------------

_RUNCARD_SRC = """\
from dataclasses import dataclass

@dataclass
class RunCard:
    sub: str = "01"
    ses: str = "pre"
    freq_lo: float = 70.0
    freq_hi: float = 200.0
"""


def test_extract_run_card_fields():
    tree = _parse(_RUNCARD_SRC)
    fields = _extract_run_card(tree)
    assert len(fields) == 4
    names = [f["field"] for f in fields]
    assert "sub" in names
    assert "freq_lo" in names


def test_extract_run_card_types():
    tree = _parse(_RUNCARD_SRC)
    fields = {f["field"]: f for f in _extract_run_card(tree)}
    assert fields["sub"]["type"] == "str"
    assert fields["freq_lo"]["type"] == "float"


def test_extract_run_card_defaults():
    tree = _parse(_RUNCARD_SRC)
    fields = {f["field"]: f for f in _extract_run_card(tree)}
    assert fields["sub"]["default"] == "'01'"
    assert fields["freq_lo"]["default"] == "70.0"


def test_extract_run_card_class_name():
    tree = _parse(_RUNCARD_SRC)
    fields = _extract_run_card(tree)
    assert all(f["class"] == "RunCard" for f in fields)


def test_extract_run_card_no_dataclass():
    tree = _parse("class Foo:\n    x: int = 1\n")
    assert _extract_run_card(tree) == []


# ---------------------------------------------------------------------------
# _extract_pipeline_context
# ---------------------------------------------------------------------------

_CTX_SRC = """\
ctx = PipelineContext.from_registry("ripple", root=project_root)
sess = ctx.session(sub="01", ses="pre")
stage = ctx.stage("filtered")
"""


def test_extract_pipeline_context_from_registry():
    tree = _parse(_CTX_SRC)
    usages = _extract_pipeline_context(tree)
    calls = [u["call"] for u in usages]
    assert "PipelineContext.from_registry" in calls


def test_extract_pipeline_context_session():
    tree = _parse(_CTX_SRC)
    usages = _extract_pipeline_context(tree)
    calls = [u["call"] for u in usages]
    assert any("session" in c for c in calls)


def test_extract_pipeline_context_stage():
    tree = _parse(_CTX_SRC)
    usages = _extract_pipeline_context(tree)
    calls = [u["call"] for u in usages]
    assert any("stage" in c for c in calls)


def test_extract_pipeline_context_kwargs():
    tree = _parse(_CTX_SRC)
    usages = {u["call"]: u for u in _extract_pipeline_context(tree)}
    assert "root" in usages["PipelineContext.from_registry"]["kwargs"]


# ---------------------------------------------------------------------------
# _build_cogpy_names and _extract_cogpy_calls
# ---------------------------------------------------------------------------

_COGPY_SRC = """\
import cogpy.brainstates as bs
from cogpy.detect import ripple
from cogpy.spectral import psd_utils as pu

result = bs.detect(signal)
r = ripple.threshold(data, threshold=3.0)
spec = pu.compute_psd(x)
not_cogpy = os.path.join("a", "b")
"""


def test_cogpy_names_alias():
    tree = _parse(_COGPY_SRC)
    imports = _extract_imports(tree)
    names = _build_cogpy_names(imports)
    assert "bs" in names
    assert names["bs"] == "cogpy.brainstates"


def test_cogpy_names_from_import():
    tree = _parse(_COGPY_SRC)
    imports = _extract_imports(tree)
    names = _build_cogpy_names(imports)
    assert "ripple" in names
    assert names["ripple"] == "cogpy.detect.ripple"


def test_cogpy_calls_detected():
    tree = _parse(_COGPY_SRC)
    imports = _extract_imports(tree)
    names = _build_cogpy_names(imports)
    calls = _extract_cogpy_calls(tree, names)
    assert any("bs.detect" in c for c in calls)
    assert any("ripple.threshold" in c for c in calls)
    assert any("pu.compute_psd" in c for c in calls)


def test_cogpy_calls_excludes_non_cogpy():
    tree = _parse(_COGPY_SRC)
    imports = _extract_imports(tree)
    names = _build_cogpy_names(imports)
    calls = _extract_cogpy_calls(tree, names)
    assert not any("os.path" in c for c in calls)


def test_cogpy_calls_deduplicated():
    src = """\
import cogpy.brainstates as bs
x = bs.detect(a)
y = bs.detect(b)
"""
    tree = _parse(src)
    imports = _extract_imports(tree)
    names = _build_cogpy_names(imports)
    calls = _extract_cogpy_calls(tree, names)
    assert calls.count("bs.detect") == 1


# ---------------------------------------------------------------------------
# analyze_notebook — integration
# ---------------------------------------------------------------------------

_FULL_NOTEBOOK = """\
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       format_name: percent
# ---

# %% [markdown]
# # Ripple Detection Investigation
#
# Investigates high-frequency ripple events in iEEG data.

# %% [markdown]
# ## Setup

# %%
from pathlib import Path
from dataclasses import dataclass
import cogpy.detect.ripple as ripple_mod
# from cogpy.brainstates import EMG  # NOT YET IMPLEMENTED

# %%
@dataclass
class RunCard:
    sub: str = "01"
    ses: str = "pre"
    band: tuple = (70, 200)

# %% [markdown]
# ## Load Data

# %%
ctx = PipelineContext.from_registry("ripple", root=Path("../.."))
sess = ctx.session(sub=rc.sub, ses=rc.ses)

# %% [markdown]
# ## Detect

# %%
events = ripple_mod.threshold(data, band=rc.band)
"""


def test_analyze_notebook_sections(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    assert "error" not in result
    assert "# Ripple Detection Investigation" in result["sections"]
    assert "## Setup" in result["sections"]
    assert "## Load Data" in result["sections"]
    assert "## Detect" in result["sections"]


def test_analyze_notebook_imports(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    modules = [i["module"] for i in result["imports"]]
    assert "pathlib" in modules
    assert "cogpy.detect.ripple" in modules


def test_analyze_notebook_run_card(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    fields = {f["field"]: f for f in result["run_card"]}
    assert "sub" in fields
    assert "ses" in fields
    assert "band" in fields
    assert fields["sub"]["default"] == "'01'"


def test_analyze_notebook_pipeline_context(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    calls = [u["call"] for u in result["pipeline_context"]]
    assert "PipelineContext.from_registry" in calls
    assert any("session" in c for c in calls)


def test_analyze_notebook_cogpy_calls(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    assert any("ripple_mod.threshold" in c for c in result["cogpy_functions"])


def test_analyze_notebook_pending_modules(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    assert any("cogpy.brainstates" in p for p in result["pending_modules"])


def test_analyze_notebook_missing_file(tmp_path):
    result = analyze_notebook(tmp_path / "nonexistent.py")
    assert "error" in result
    assert "not found" in result["error"]


def test_analyze_notebook_nb_path(tmp_path):
    nb = tmp_path / "investigate_ripple.py"
    nb.write_text(_FULL_NOTEBOOK, encoding="utf-8")
    result = analyze_notebook(nb)
    assert result["nb_path"] == str(nb)
