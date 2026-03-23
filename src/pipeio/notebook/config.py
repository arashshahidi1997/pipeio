"""Notebook configuration: load and validate ``notebook.yml``."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class NotebookEntry(BaseModel):
    """A single notebook entry in ``notebook.yml``."""

    path: str
    pair_ipynb: bool = False
    pair_myst: bool = False
    publish_myst: bool = False
    publish_html: bool = False


class PublishConfig(BaseModel):
    """Publication settings for notebooks."""

    docs_dir: str = ""
    prefix: str = "nb-"


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
