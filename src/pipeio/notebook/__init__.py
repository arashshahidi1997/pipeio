"""Notebook lifecycle management: pair, sync, execute, publish.

Supports multiple notebook backends (jupytext percent-format, marimo)
via the :class:`NotebookBackend` protocol.
"""

from pipeio.notebook.backend import (
    NotebookBackend,
    detect_format,
    get_backend,
    list_backends,
    resolve_backend,
)
from pipeio.notebook.lifecycle import (
    find_notebook_configs,
    nb_exec,
    nb_pair,
    nb_publish,
    nb_status,
    nb_sync,
)

__all__ = [
    # Backend abstraction
    "NotebookBackend",
    "detect_format",
    "get_backend",
    "list_backends",
    "resolve_backend",
    # Lifecycle operations
    "find_notebook_configs",
    "nb_exec",
    "nb_pair",
    "nb_publish",
    "nb_status",
    "nb_sync",
]
