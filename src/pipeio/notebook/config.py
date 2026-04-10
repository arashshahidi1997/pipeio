"""Notebook configuration: load and validate ``notebook.yml``."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class NotebookEntry(BaseModel):
    """A single notebook entry in ``notebook.yml``."""

    path: str
    kind: str = ""  # investigate | explore | demo | validate | interactive
    description: str = ""
    status: str = "active"  # draft | active | stale | promoted | archived
    format: str = ""  # "" (auto-detect) | "percent" | "marimo"
    kernel: str = ""  # Jupyter kernelspec (percent-only; ignored for marimo)
    mod: str = ""
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

    kernel: str = ""
    default_format: str = ""  # flow-level default: "percent" | "marimo" | ""
    publish: PublishConfig = Field(default_factory=PublishConfig)
    entries: list[NotebookEntry] = Field(default_factory=list)

    def resolve_format(self, entry: NotebookEntry) -> str:
        """Return effective format: entry-level > flow-level > auto-detect."""
        return entry.format or self.default_format or ""

    def resolve_kernel(self, entry: NotebookEntry) -> str:
        """Return the effective kernel for *entry*.

        Returns empty string for marimo notebooks (they don't use Jupyter kernels).
        """
        fmt = self.resolve_format(entry)
        if fmt == "marimo":
            return ""
        return entry.kernel or self.kernel

    @classmethod
    def from_yaml(cls, path: Path) -> NotebookConfig:
        """Load notebook config from a YAML file."""
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}
        return cls.model_validate(raw)

    def to_yaml(self, path: Path) -> None:
        """Write notebook config to a YAML file."""
        data = self.model_dump(exclude_defaults=True)
        # Always include entries even if empty
        if "entries" not in data:
            data["entries"] = []
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            yaml.safe_dump(data, fh, default_flow_style=False, sort_keys=False)
