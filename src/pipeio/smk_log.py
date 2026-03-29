"""Standardized Snakemake script logging for pipeio pipelines.

Snakemake does not automatically redirect stdout/stderr into ``log:`` for
``script:`` rules.  This module configures Python's :mod:`logging` to write
to both the Snakemake log file and stderr, ensuring that script output is
captured and formatted consistently across all pipeline rules.

Usage inside a Snakemake ``script:`` rule::

    from pipeio.smk_log import setup_logging

    logger, log_path = setup_logging(snakemake)
    logger.info("Starting work …")
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Optional


def setup_logging(
    snakemake: Any | None,
    *,
    name: str = "pipeline",
    level: str = "INFO",
) -> tuple[logging.Logger, Optional[Path]]:
    """Configure logging for a Snakemake ``script:`` rule.

    Sets up handlers for both the Snakemake log file (if present) and stderr.
    Installs a custom :func:`sys.excepthook` so that unhandled exceptions are
    captured in the log file.

    Parameters
    ----------
    snakemake
        The ``snakemake`` object injected by Snakemake into ``script:`` rules.
        May be ``None`` for standalone / testing use.
    name
        Logger name.  Defaults to ``"pipeline"``.
    level
        Log level string (e.g. ``"INFO"``, ``"DEBUG"``).

    Returns
    -------
    tuple[logging.Logger, Path | None]
        The configured logger and the resolved log file path (``None`` when no
        Snakemake log target is available).
    """
    log_level = getattr(logging, str(level).upper(), logging.INFO)

    log_path: Optional[Path] = None
    try:
        if snakemake is not None and getattr(snakemake, "log", None):
            log_path = Path(str(snakemake.log[0]))
    except Exception:
        log_path = None

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stderr)]
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.insert(
            0, logging.FileHandler(log_path, mode="w", encoding="utf-8")
        )

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
    logging.captureWarnings(True)

    logger = logging.getLogger(name)
    if log_path is not None:
        logger.info("Logging to %s", log_path)

    # Ensure unhandled exceptions end up in the Snakemake log file.
    def _excepthook(
        exc_type: type[BaseException],
        exc: BaseException,
        tb: Any,
    ) -> None:
        logger.error("Unhandled exception", exc_info=(exc_type, exc, tb))
        sys.__excepthook__(exc_type, exc, tb)

    sys.excepthook = _excepthook
    return logger, log_path
