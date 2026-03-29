"""Tests for pipeio.smk_log — Snakemake script logging."""

import logging
import sys
from types import SimpleNamespace

import pytest

from pipeio.smk_log import setup_logging


def test_setup_logging_no_snakemake():
    """When snakemake is None, returns logger with no file handler."""
    logger, log_path = setup_logging(None, name="test_none")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_none"
    assert log_path is None


def test_setup_logging_with_log_file(tmp_path):
    """When snakemake.log is set, creates a file handler at that path."""
    log_file = tmp_path / "sub" / "rule.log"
    snakemake = SimpleNamespace(log=[str(log_file)])

    logger, log_path = setup_logging(snakemake, name="test_file")

    assert log_path == log_file
    assert log_file.parent.exists()
    logger.info("hello from test")
    # Flush handlers so content is written
    for h in logging.root.handlers:
        h.flush()
    assert log_file.exists()
    assert "hello from test" in log_file.read_text()


def test_setup_logging_empty_log():
    """When snakemake.log is empty, behaves like no log file."""
    snakemake = SimpleNamespace(log=[])
    logger, log_path = setup_logging(snakemake, name="test_empty")
    assert log_path is None


def test_setup_logging_custom_level():
    """Respects the level parameter."""
    logger, _ = setup_logging(None, name="test_level", level="DEBUG")
    assert logger.getEffectiveLevel() <= logging.DEBUG


def test_excepthook_installed(tmp_path):
    """setup_logging installs a custom excepthook."""
    log_file = tmp_path / "exc.log"
    snakemake = SimpleNamespace(log=[str(log_file)])
    setup_logging(snakemake, name="test_exc")
    assert sys.excepthook is not sys.__excepthook__


def test_public_api():
    """setup_logging is importable from pipeio top-level."""
    from pipeio import setup_logging as top_level
    assert top_level is setup_logging
