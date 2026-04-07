"""Flow / mod registry: scan, load, validate, query.

The registry maps flows and their mods to filesystem paths, config files,
and documentation locations.  Each flow is a self-contained unit of work;
the old "pipe" grouping layer has been removed.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def slug_ok(name: str) -> bool:
    """Return True if *name* is a valid slug (lowercase, underscores)."""
    return bool(_SLUG_RE.match(name))


def find_registry(root: Path) -> Path | None:
    """Locate the pipeline registry, checking .projio/pipeio/ first.

    Returns the path to the registry file, or ``None`` if not found.
    """
    for candidate in (
        root / ".projio" / "pipeio" / "registry.yml",
        root / ".pipeio" / "registry.yml",
    ):
        if candidate.exists():
            return candidate
    return None


class ValidationResult(BaseModel):
    """Result of a registry validation run."""

    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


class ModEntry(BaseModel):
    """A single mod (logical module) within a flow."""

    name: str
    rules: list[str] = Field(default_factory=list)
    doc_path: str | None = None


class FlowEntry(BaseModel):
    """A flow: a concrete workflow with its own Snakefile and config."""

    name: str
    code_path: str
    config_path: str | None = None
    doc_path: str | None = None
    mods: dict[str, ModEntry] = Field(default_factory=dict)
    app_type: str = ""  # "snakebids", "snakemake", or "" (unknown/legacy)


class PipelineRegistry(BaseModel):
    """Top-level registry of all pipes, flows, and mods."""

    flows: dict[str, FlowEntry] = Field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: Path) -> PipelineRegistry:
        """Load a registry from a YAML file."""
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}
        return cls.model_validate(raw)

    def to_yaml(self, path: Path) -> None:
        """Serialize the registry to a YAML file."""
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "flows": {
                key: entry.model_dump() for key, entry in self.flows.items()
            },
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            fh.write("# pipeio pipeline registry\n")
            yaml.safe_dump(payload, fh, sort_keys=False, default_flow_style=False)

    def list_flows(self, prefix: str | None = None) -> list[FlowEntry]:
        """Return flows, optionally filtered by name prefix."""
        entries = list(self.flows.values())
        if prefix:
            entries = [f for f in entries if f.name.startswith(prefix)]
        return entries

    def remove(self, flow: str) -> FlowEntry:
        """Remove a flow from the registry and return the removed entry."""
        if flow not in self.flows:
            # Try matching by name field
            for k, entry in self.flows.items():
                if entry.name == flow:
                    return self.flows.pop(k)
            raise KeyError(f"Flow not found: {flow!r}")
        return self.flows.pop(flow)

    def get(self, flow: str) -> FlowEntry:
        """Look up a flow by name.

        Raises KeyError if the flow is not found.
        """
        if flow in self.flows:
            return self.flows[flow]
        # Fallback: match by entry.name
        for entry in self.flows.values():
            if entry.name == flow:
                return entry
        known = ", ".join(sorted(self.flows.keys())) or "(none)"
        raise KeyError(f"Unknown flow: {flow!r}. Known: {known}")

    @classmethod
    def scan(
        cls,
        pipelines_dir: Path,
        docs_dir: Path | None = None,
        ignore: set[str] | None = None,
    ) -> PipelineRegistry:
        """Discover flows from the filesystem and return a new registry.

        Scans *pipelines_dir* for directories containing Snakefile or
        config.yml. Optionally cross-references *docs_dir* for doc paths.

        For nested dirs like ``preprocess/ieeg/``, the flow name is the
        deepest directory name (e.g. ``ieeg``).  For flat dirs like
        ``brainstate/``, flow name = dir name.

        Parameters
        ----------
        ignore : set[str] | None
            Flow names to skip during scan.
        """
        flows: dict[str, FlowEntry] = {}

        if not pipelines_dir.exists():
            return cls(flows={})

        for top_dir in sorted(
            p for p in pipelines_dir.iterdir()
            if p.is_dir() and not p.name.startswith("_") and p.name != "__pycache__"
        ):
            discovered = _discover_flows(top_dir, docs_dir)
            if ignore:
                discovered = {k: v for k, v in discovered.items() if k not in ignore}
            flows.update(discovered)

        return cls(flows=flows)

    def validate(self, root: Path | None = None) -> ValidationResult:
        """Validate registry consistency. Returns errors and warnings."""
        errors: list[str] = []
        warnings: list[str] = []
        seen_names: set[str] = set()

        for key, entry in self.flows.items():
            flow_id = entry.name

            # Slug check
            if not slug_ok(entry.name):
                warnings.append(f"Non-canonical flow slug: {entry.name!r}")

            # Name uniqueness
            if flow_id in seen_names:
                errors.append(f"Duplicate flow name: {flow_id}")
            seen_names.add(flow_id)

            # Filesystem checks (only if root provided)
            if root:
                if not (root / entry.code_path).exists():
                    errors.append(
                        f"Code path does not exist: {entry.code_path} "
                        f"(flow {flow_id})"
                    )
                if entry.config_path and not (root / entry.config_path).exists():
                    errors.append(
                        f"Config path does not exist: {entry.config_path} "
                        f"(flow {flow_id})"
                    )
                if entry.doc_path and not (root / entry.doc_path).exists():
                    warnings.append(
                        f"Doc path does not exist: {entry.doc_path} "
                        f"(flow {flow_id})"
                    )

            # Mod slug checks
            for mod_name in entry.mods:
                if not slug_ok(mod_name):
                    warnings.append(
                        f"Non-canonical mod slug: {mod_name!r} "
                        f"(flow {flow_id})"
                    )

        return ValidationResult(errors=errors, warnings=warnings)


def _discover_flows(
    top_dir: Path,
    docs_dir: Path | None,
) -> dict[str, FlowEntry]:
    """Discover flows within a directory.

    For a flat directory (has Snakefile at root), flow name = dir name.
    For nested dirs, each child with a Snakefile becomes a flow keyed by
    the child's name.
    """
    flows: dict[str, FlowEntry] = {}

    # Check if top_dir itself is a flow (has Snakefile or config.yml at root)
    if (top_dir / "Snakefile").exists() or (top_dir / "config.yml").exists():
        name = top_dir.name
        doc_path = _find_doc_path(docs_dir, name) if docs_dir else None
        config_path = _resolve_config_path(top_dir)
        flows[name] = FlowEntry(
            name=name,
            code_path=str(top_dir),
            config_path=config_path,
            doc_path=doc_path,
            mods=_discover_mods(top_dir),
            app_type=_detect_app_type(top_dir),
        )

    # Check subdirectories for additional flows
    for child in sorted(p for p in top_dir.iterdir() if p.is_dir()):
        if child.name.startswith("_") or child.name == "__pycache__":
            continue
        if (child / "Snakefile").exists() or (child / "config.yml").exists():
            name = child.name
            if name in flows:
                continue
            doc_path = _find_doc_path(docs_dir, name) if docs_dir else None
            config_path = _resolve_config_path(child)
            flows[name] = FlowEntry(
                name=name,
                code_path=str(child),
                config_path=config_path,
                doc_path=doc_path,
                mods=_discover_mods(child),
                app_type=_detect_app_type(child),
            )

    # Check for .smk files at top_dir root
    for smk in sorted(top_dir.glob("*.smk")):
        name = smk.stem
        if name in flows:
            continue
        doc_path = _find_doc_path(docs_dir, name) if docs_dir else None
        config_path = _resolve_config_path(top_dir)
        flows[name] = FlowEntry(
            name=name,
            code_path=str(top_dir),
            config_path=config_path,
            doc_path=doc_path,
            app_type=_detect_app_type(top_dir),
        )

    return flows


def _detect_app_type(flow_dir: Path) -> str:
    """Return app_type for a flow directory: 'snakebids', 'snakemake', or ''."""
    if (flow_dir / "run.py").exists():
        return "snakebids"
    if (flow_dir / "Snakefile").exists():
        return "snakemake"
    return ""


def _resolve_config_path(flow_dir: Path) -> str | None:
    """Return the config path for a flow, preferring config/snakebids.yml over config.yml."""
    snakebids_cfg = flow_dir / "config" / "snakebids.yml"
    if snakebids_cfg.exists():
        return str(snakebids_cfg)
    plain_cfg = flow_dir / "config.yml"
    if plain_cfg.exists():
        return str(plain_cfg)
    return None


_RULE_RE = re.compile(r"^\s*rule\s+(\w+)\s*:", re.MULTILINE)
_MOD_PREFIX_RE = re.compile(r"^([a-z][a-z0-9]*?)_")


def _discover_mods(flow_dir: Path) -> dict[str, ModEntry]:
    """Parse Snakefiles for ``rule`` blocks and group them into mods by prefix.

    Mod grouping convention:
    - Rules named ``<prefix>_<rest>`` are grouped under mod ``<prefix>``.
    - Rules with no underscore become a mod named after the rule itself.
    - .smk files in the flow directory are also scanned.
    """
    rule_names: list[str] = []

    # Scan Snakefile and *.smk files
    candidates = list(flow_dir.glob("*.smk"))
    snakefile = flow_dir / "Snakefile"
    if snakefile.exists():
        candidates.insert(0, snakefile)

    for sf in candidates:
        try:
            text = sf.read_text(encoding="utf-8")
        except Exception:
            continue
        rule_names.extend(_RULE_RE.findall(text))

    if not rule_names:
        return {}

    # Group rules by mod prefix
    mod_rules: dict[str, list[str]] = {}
    for rule in rule_names:
        m = _MOD_PREFIX_RE.match(rule)
        mod_name = m.group(1) if m else rule
        mod_rules.setdefault(mod_name, []).append(rule)

    # Check for per-mod docs in flow_dir/docs/
    mods: dict[str, ModEntry] = {}
    for mod_name, rules in sorted(mod_rules.items()):
        doc_path = flow_dir / "docs" / f"mod-{mod_name}.md"
        mods[mod_name] = ModEntry(
            name=mod_name,
            rules=sorted(rules),
            doc_path=str(doc_path) if doc_path.exists() else None,
        )

    return mods


def _find_doc_path(docs_dir: Path | None, flow: str) -> str | None:
    """Look for documentation for a flow in the docs directory."""
    if docs_dir is None or not docs_dir.exists():
        return None
    flow_doc = docs_dir / flow
    if flow_doc.is_dir():
        return str(flow_doc)
    return None
