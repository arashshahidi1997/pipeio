"""Notebook configuration: load and validate ``notebook.yml``."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class NotebookEntry(BaseModel):
    """A single notebook entry in ``notebook.yml``."""

    path: str
    kind: str = ""
    description: str = ""
    status: str = "active"
    pair_ipynb: bool = False
    pair_myst: bool = False
    publish_myst: bool = False
    publish_html: bool = False


class PublishConfig(BaseModel):
    """Publication settings for notebooks.

    When used with ``pipeio docs collect`` (preferred), *docs_dir* is
    computed from the registry as ``docs/pipelines/<pipe>/<flow>/notebooks/``.
    The *docs_dir* field is kept for standalone ``pipeio nb publish`` usage.
    """

    format: str = "html"
    docs_dir: str = ""
    prefix: str = ""


class NotebookConfig(BaseModel):
    """Schema for a flow's ``notebook.yml``."""

    publish: PublishConfig = Field(default_factory=PublishConfig)
    entries: list[NotebookEntry] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: Path) -> NotebookConfig:
        """Load notebook config from a YAML file."""
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}
        return cls.model_validate(raw)
