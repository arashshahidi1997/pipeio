"""Notebook backend abstraction: Protocol, registry, format detection.

Each backend handles one notebook format (percent-format via jupytext,
or marimo).  Higher-level lifecycle operations (audit, lab, status) compose
these primitives and remain format-agnostic.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class NotebookBackend(Protocol):
    """Format-specific operations for a notebook backend."""

    name: str  # "percent" or "marimo"

    def detect(self, py_path: Path) -> bool:
        """Return True if *py_path* is a notebook in this format."""
        ...

    def output_paths(self, py_path: Path) -> dict[str, Path]:
        """Return format-dependent output paths.

        For percent: ``{"ipynb": ..., "myst": ...}``
        For marimo: ``{}`` (single-file format, no paired outputs)
        """
        ...

    def sync(
        self,
        py_path: Path,
        *,
        direction: str = "py2nb",
        formats: list[str] | None = None,
        force: bool = False,
        kernel: str = "",
        python_bin: "str | list[str] | None" = None,
    ) -> dict[str, Any]:
        """Sync between source and paired formats.

        For percent: jupytext bidirectional sync.
        For marimo: no-op (single-file format).
        """
        ...

    def execute(
        self,
        py_path: Path,
        *,
        kernel: str = "",
        params: dict[str, Any] | None = None,
        timeout: int = 600,
        cwd: Path | None = None,
    ) -> dict[str, Any]:
        """Execute the notebook and return status + diagnostics.

        For percent: papermill on the paired .ipynb.
        For marimo: ``marimo run <py_path>``.
        """
        ...

    def validate(self, py_path: Path) -> dict[str, Any]:
        """Structural validation.

        For percent: AST syntax check on each cell, import isolation check.
        For marimo: ``marimo check <py_path>``.
        """
        ...

    def export(
        self,
        py_path: Path,
        *,
        output_format: str,
        output_path: Path,
    ) -> dict[str, Any]:
        """Export to a target format (html, markdown, ipynb).

        For percent: nbconvert on the paired .ipynb.
        For marimo: ``marimo export <format> <py_path> -o <output_path>``.
        """
        ...

    def split_cells(self, source: str) -> list[tuple[str, str]]:
        """Split notebook source into ``[(kind, content), ...]``.

        *kind* is ``'code'`` or ``'markdown'``.
        """
        ...

    def template(
        self,
        *,
        name: str,
        flow: str,
        kind: str,
        description: str,
        config_path: str = "",
        groups: list[str] | None = None,
        output_dir: str = "",
        compute_lib: str = "",
    ) -> str:
        """Generate notebook source content for scaffolding.

        Returns the full file content as a string.
        """
        ...


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_BACKENDS: dict[str, NotebookBackend] = {}


def _init_backends() -> None:
    """Lazily populate the backend registry."""
    if _BACKENDS:
        return

    from pipeio.notebook.backend_percent import PercentBackend
    _BACKENDS["percent"] = PercentBackend()

    try:
        from pipeio.notebook.backend_marimo import MarimoBackend
        _BACKENDS["marimo"] = MarimoBackend()
    except Exception:
        pass  # marimo not installed — graceful degradation


def get_backend(fmt: str) -> NotebookBackend:
    """Return the backend for *fmt*, raising ``ValueError`` if unknown."""
    _init_backends()
    if fmt not in _BACKENDS:
        available = ", ".join(sorted(_BACKENDS)) or "(none)"
        raise ValueError(
            f"Unknown notebook format: {fmt!r}. Available: {available}"
        )
    return _BACKENDS[fmt]


def list_backends() -> list[str]:
    """Return names of available backends."""
    _init_backends()
    return sorted(_BACKENDS)


def detect_format(py_path: Path) -> str:
    """Auto-detect notebook format from file content.

    Checks marimo first (more specific signature), then percent-format.
    Returns ``"percent"`` as the default fallback.
    """
    _init_backends()
    # Marimo has a distinctive signature — check first to avoid false positives
    if "marimo" in _BACKENDS and _BACKENDS["marimo"].detect(py_path):
        return "marimo"
    if "percent" in _BACKENDS and _BACKENDS["percent"].detect(py_path):
        return "percent"
    return "percent"


def resolve_backend(
    format_hint: str,
    py_path: Path | None = None,
) -> NotebookBackend:
    """Resolve backend from explicit format or auto-detection.

    Parameters
    ----------
    format_hint : str
        Explicit format (``"percent"``, ``"marimo"``, or ``""`` for auto-detect).
    py_path : Path | None
        Path to the ``.py`` file (used for auto-detection when *format_hint*
        is empty).
    """
    if format_hint:
        return get_backend(format_hint)
    if py_path is not None and py_path.exists():
        return get_backend(detect_format(py_path))
    return get_backend("percent")
