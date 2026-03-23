"""Basic smoke tests for pipeio."""

import pipeio


def test_import():
    assert hasattr(pipeio, "__all__")
