"""Tests for the marimo notebook backend.

Tests detection, cell splitting, template generation, and validation
without requiring marimo to be installed (subprocess calls are mocked).
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

MARIMO_SOURCE = textwrap.dedent("""\
    import marimo

    app = marimo.App(width="medium")

    @app.cell
    def setup():
        from pathlib import Path
        return (Path,)

    @app.cell
    def analysis(Path):
        data = Path("data.csv")
        return (data,)

    @app.cell
    def display(mo, data):
        mo.md(f"# Results for {data}")

    if __name__ == "__main__":
        app.run()
""")

PERCENT_SOURCE = textwrap.dedent("""\
    # ---
    # jupyter:
    #   jupytext:
    #     text_representation:
    #       format_name: percent
    # ---

    # %% [markdown]
    # # Title

    # %%
    import os
    x = 1
""")


def _make_marimo_file(tmp_path: Path) -> Path:
    p = tmp_path / "notebook.py"
    p.write_text(MARIMO_SOURCE, encoding="utf-8")
    return p


def _make_percent_file(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    p = tmp_path / "notebook.py"
    p.write_text(PERCENT_SOURCE, encoding="utf-8")
    return p


class TestMarimoDetection:
    """Test format detection for marimo notebooks."""

    def test_detect_marimo_source(self, tmp_path):
        from pipeio.notebook.backend_percent import PercentBackend

        pb = PercentBackend()
        mp = _make_marimo_file(tmp_path)
        pp = _make_percent_file(tmp_path / "sub")

        # PercentBackend should NOT detect marimo (no # %% markers)
        assert not pb.detect(mp)
        assert pb.detect(pp)

    def test_detect_format_auto(self, tmp_path):
        from pipeio.notebook.backend import detect_format, _BACKENDS, _init_backends

        # Clear backend cache to force re-init
        _BACKENDS.clear()

        mp = _make_marimo_file(tmp_path)
        pp = _make_percent_file(tmp_path / "sub")

        # If marimo backend is available, it detects marimo files
        _init_backends()
        if "marimo" in _BACKENDS:
            assert detect_format(mp) == "marimo"
        assert detect_format(pp) == "percent"

    def test_detect_nonexistent(self, tmp_path):
        from pipeio.notebook.backend_percent import PercentBackend

        pb = PercentBackend()
        assert not pb.detect(tmp_path / "nonexistent.py")


# ---------------------------------------------------------------------------
# Cell splitting
# ---------------------------------------------------------------------------

class TestMarimoCellSplitting:
    """Test cell splitting for marimo format."""

    def test_split_cells_basic(self):
        # Import MarimoBackend with mocked subprocess
        with patch("pipeio.notebook.backend_marimo.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            from pipeio.notebook.backend_marimo import MarimoBackend
            backend = MarimoBackend.__new__(MarimoBackend)
            backend._marimo_cmd = ["marimo"]

        cells = backend.split_cells(MARIMO_SOURCE)
        assert len(cells) >= 2  # at least setup + analysis cells

        # Check that we get code cells
        kinds = [k for k, _ in cells]
        assert "code" in kinds

    def test_split_cells_markdown_detection(self):
        """mo.md() calls should be classified as markdown."""
        with patch("pipeio.notebook.backend_marimo.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            from pipeio.notebook.backend_marimo import MarimoBackend
            backend = MarimoBackend.__new__(MarimoBackend)
            backend._marimo_cmd = ["marimo"]

        source = textwrap.dedent("""\
            import marimo
            app = marimo.App()

            @app.cell
            def title(mo):
                mo.md("# Hello World")

            @app.cell
            def compute():
                x = 42
                return (x,)
        """)
        cells = backend.split_cells(source)
        kinds = [k for k, _ in cells]
        assert "markdown" in kinds
        assert "code" in kinds


# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------

class TestMarimoOutputPaths:

    def test_output_paths_empty(self, tmp_path):
        with patch("pipeio.notebook.backend_marimo.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            from pipeio.notebook.backend_marimo import MarimoBackend
            backend = MarimoBackend.__new__(MarimoBackend)
            backend._marimo_cmd = ["marimo"]

        assert backend.output_paths(tmp_path / "nb.py") == {}


# ---------------------------------------------------------------------------
# Sync (no-op)
# ---------------------------------------------------------------------------

class TestMarimoSync:

    def test_sync_noop(self, tmp_path):
        with patch("pipeio.notebook.backend_marimo.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            from pipeio.notebook.backend_marimo import MarimoBackend
            backend = MarimoBackend.__new__(MarimoBackend)
            backend._marimo_cmd = ["marimo"]

        result = backend.sync(tmp_path / "nb.py")
        assert result["skipped"] is True
        assert "single-file" in result["reason"]


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------

class TestMarimoTemplate:

    def test_template_basic(self):
        with patch("pipeio.notebook.backend_marimo.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            from pipeio.notebook.backend_marimo import MarimoBackend
            backend = MarimoBackend.__new__(MarimoBackend)
            backend._marimo_cmd = ["marimo"]

        content = backend.template(
            name="investigate_noise",
            flow="preprocess_ieeg",
            kind="investigate",
            description="Test noise characterization",
        )

        assert "import marimo" in content
        assert "marimo.App(" in content
        assert "@app.cell" in content
        assert "Investigate Noise" in content
        assert 'if __name__ == "__main__"' in content

    def test_template_with_compute_lib(self):
        with patch("pipeio.notebook.backend_marimo.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            from pipeio.notebook.backend_marimo import MarimoBackend
            backend = MarimoBackend.__new__(MarimoBackend)
            backend._marimo_cmd = ["marimo"]

        content = backend.template(
            name="test",
            flow="myflow",
            kind="explore",
            description="",
            compute_lib="cogpy",
        )

        assert "import cogpy" in content


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

def _make_marimo_backend():
    """Create a MarimoBackend without invoking subprocess for __init__."""
    from pipeio.notebook.backend_marimo import MarimoBackend
    backend = MarimoBackend.__new__(MarimoBackend)
    backend._marimo_cmd = ["marimo"]
    return backend


class TestMarimoValidate:

    @patch("pipeio.notebook.backend_marimo.subprocess.run")
    def test_validate_success(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        backend = _make_marimo_backend()
        nb = _make_marimo_file(tmp_path)
        result = backend.validate(nb)
        assert result["valid"] is True
        assert result["format"] == "marimo"

    @patch("pipeio.notebook.backend_marimo.subprocess.run")
    def test_validate_failure(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error: redefined variable"
        )
        backend = _make_marimo_backend()
        nb = _make_marimo_file(tmp_path)
        result = backend.validate(nb)
        assert result["valid"] is False

    def test_validate_missing_file(self, tmp_path):
        backend = _make_marimo_backend()
        result = backend.validate(tmp_path / "nonexistent.py")
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class TestMarimoExecute:

    @patch("pipeio.notebook.backend_marimo.subprocess.run")
    def test_execute_success(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stdout="Done", stderr="")
        backend = _make_marimo_backend()
        nb = _make_marimo_file(tmp_path)
        result = backend.execute(nb)
        assert result["executed"] is True

    def test_execute_missing(self, tmp_path):
        backend = _make_marimo_backend()
        result = backend.execute(tmp_path / "nonexistent.py")
        assert result["executed"] is False


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

class TestMarimoExport:

    @patch("pipeio.notebook.backend_marimo.subprocess.run")
    def test_export_html(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0)
        backend = _make_marimo_backend()
        nb = _make_marimo_file(tmp_path)
        out = tmp_path / "output.html"
        result = backend.export(nb, output_format="html", output_path=out)
        assert result["exported"] is True


# ---------------------------------------------------------------------------
# Config integration
# ---------------------------------------------------------------------------

class TestConfigIntegration:

    def test_notebook_entry_format_field(self):
        from pipeio.notebook.config import NotebookEntry
        entry = NotebookEntry(path="notebooks/.src/test.py", format="marimo")
        assert entry.format == "marimo"

    def test_notebook_entry_format_default(self):
        from pipeio.notebook.config import NotebookEntry
        entry = NotebookEntry(path="notebooks/.src/test.py")
        assert entry.format == ""

    def test_notebook_config_resolve_format(self):
        from pipeio.notebook.config import NotebookConfig, NotebookEntry
        cfg = NotebookConfig(default_format="marimo")
        entry = NotebookEntry(path="test.py")
        assert cfg.resolve_format(entry) == "marimo"

        # Entry-level overrides flow-level
        entry2 = NotebookEntry(path="test.py", format="percent")
        assert cfg.resolve_format(entry2) == "percent"

    def test_notebook_config_resolve_kernel_marimo(self):
        from pipeio.notebook.config import NotebookConfig, NotebookEntry
        cfg = NotebookConfig(kernel="cogpy", default_format="marimo")
        entry = NotebookEntry(path="test.py")
        # Marimo notebooks don't use Jupyter kernels
        assert cfg.resolve_kernel(entry) == ""

    def test_notebook_config_resolve_kernel_percent(self):
        from pipeio.notebook.config import NotebookConfig, NotebookEntry
        cfg = NotebookConfig(kernel="cogpy")
        entry = NotebookEntry(path="test.py")
        assert cfg.resolve_kernel(entry) == "cogpy"


# ---------------------------------------------------------------------------
# PercentBackend validate
# ---------------------------------------------------------------------------

class TestPercentValidate:

    def test_validate_clean(self, tmp_path):
        from pipeio.notebook.backend_percent import PercentBackend
        pb = PercentBackend()

        nb = tmp_path / "clean.py"
        nb.write_text(textwrap.dedent("""\
            # %%
            import numpy as np

            # %%
            x = np.array([1, 2, 3])
        """), encoding="utf-8")

        result = pb.validate(nb)
        assert result["valid"] is True
        assert result["format"] == "percent"

    def test_validate_syntax_error(self, tmp_path):
        from pipeio.notebook.backend_percent import PercentBackend
        pb = PercentBackend()

        nb = tmp_path / "broken.py"
        nb.write_text(textwrap.dedent("""\
            # %%
            def foo(
        """), encoding="utf-8")

        result = pb.validate(nb)
        assert result["valid"] is False
        assert any(i["severity"] == "error" for i in result["issues"])

    def test_validate_import_isolation(self, tmp_path):
        from pipeio.notebook.backend_percent import PercentBackend
        pb = PercentBackend()

        nb = tmp_path / "mixed.py"
        nb.write_text(textwrap.dedent("""\
            # %%
            import numpy as np
            x = np.array([1, 2, 3])
        """), encoding="utf-8")

        result = pb.validate(nb)
        # Should warn about mixing imports with executable code
        warnings = [i for i in result["issues"] if i["severity"] == "warning"]
        assert any("mixes imports" in w["message"] for w in warnings)
