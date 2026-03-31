"""Pipe / flow / mod registry: scan, load, validate, query.

The registry maps the three-level hierarchy (pipe / flow / mod) to filesystem
paths, config files, and documentation locations.
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
    pipe: str
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

    def list_pipes(self) -> list[str]:
        """Return sorted unique pipe names."""
        return sorted({f.pipe for f in self.flows.values()})

    def list_flows(self, pipe: str | None = None) -> list[FlowEntry]:
        """Return flows, optionally filtered by pipe."""
        entries = list(self.flows.values())
        if pipe:
            entries = [f for f in entries if f.pipe == pipe]
        return entries

    def remove(self, pipe: str, flow: str) -> FlowEntry:
        """Remove a flow from the registry and return the removed entry."""
        key = f"{pipe}/{flow}"
        if key not in self.flows:
            # Try matching by pipe+name fields
            for k, entry in self.flows.items():
                if entry.pipe == pipe and entry.name == flow:
                    key = k
                    break
            else:
                raise KeyError(f"Flow not found: pipe={pipe!r} flow={flow!r}")
        return self.flows.pop(key)

    def get(self, pipe: str, flow: str | None = None) -> FlowEntry:
        """Resolve a (pipe, flow) pair to a FlowEntry.

        Flow resolution logic (matching pixecog):
        - If *flow* is given explicitly, look it up directly.
        - If omitted and the pipe has exactly one flow, auto-select it.
        - If omitted and a flow with the same name as the pipe exists, use it.
        - Otherwise raise ValueError listing the available flows.
        """
        pipe_flows = [f for f in self.flows.values() if f.pipe == pipe]
        if not pipe_flows:
            known = ", ".join(self.list_pipes()) or "(none)"
            raise KeyError(f"Unknown pipe: {pipe!r}. Known: {known}")

        if flow is not None:
            for f in pipe_flows:
                if f.name == flow:
                    return f
            known = ", ".join(f.name for f in pipe_flows)
            raise KeyError(
                f"Unknown flow for pipe={pipe!r}: {flow!r}. Known: {known}"
            )

        # Auto-select: single flow
        if len(pipe_flows) == 1:
            return pipe_flows[0]

        # Auto-select: flow name == pipe name
        for f in pipe_flows:
            if f.name == pipe:
                return f

        known = ", ".join(f.name for f in pipe_flows)
        raise ValueError(
            f"flow is required for pipe={pipe!r}. Known flows: {known}"
        )

    @classmethod
    def scan(
        cls,
        pipelines_dir: Path,
        docs_dir: Path | None = None,
        ignore: set[str] | None = None,
    ) -> PipelineRegistry:
        """Discover flows from the filesystem and return a new registry.

        Scans *pipelines_dir* for pipe/flow directories containing Snakefile
        or config.yml. Optionally cross-references *docs_dir* for doc paths.

        Parameters
        ----------
        ignore : set[str] | None
            Flow keys (``"pipe/flow"``) to skip during scan.
        """
        flows: dict[str, FlowEntry] = {}

        if not pipelines_dir.exists():
            return cls(flows={})

        for pipe_dir in sorted(
            p for p in pipelines_dir.iterdir()
            if p.is_dir() and not p.name.startswith("_") and p.name != "__pycache__"
        ):
            pipe = pipe_dir.name
            discovered = _discover_flows(pipe_dir, pipe, docs_dir)
            if ignore:
                discovered = {k: v for k, v in discovered.items() if k not in ignore}
            flows.update(discovered)

        return cls(flows=flows)

    def validate(self, root: Path | None = None) -> ValidationResult:
        """Validate registry consistency. Returns errors and warnings."""
        errors: list[str] = []
        warnings: list[str] = []
        seen_ids: set[str] = set()

        for key, entry in self.flows.items():
            flow_id = f"{entry.pipe}/{entry.name}"

            # Slug checks
            if not slug_ok(entry.pipe):
                warnings.append(f"Non-canonical pipe slug: {entry.pipe!r}")
            if not slug_ok(entry.name):
                warnings.append(f"Non-canonical flow slug: {entry.name!r}")

            # ID uniqueness
            if flow_id in seen_ids:
                errors.append(f"Duplicate flow ID: {flow_id}")
            seen_ids.add(flow_id)

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
    pipe_dir: Path,
    pipe: str,
    docs_dir: Path | None,
) -> dict[str, FlowEntry]:
    """Discover flows within a single pipe directory."""
    flows: dict[str, FlowEntry] = {}

    # Check if pipe_dir itself is a flow (has Snakefile or config.yml at root)
    if (pipe_dir / "Snakefile").exists() or (pipe_dir / "config.yml").exists():
        key = pipe
        doc_path = _find_doc_path(docs_dir, pipe, pipe) if docs_dir else None
        config_path = _resolve_config_path(pipe_dir)
        flows[key] = FlowEntry(
            name=pipe,
            pipe=pipe,
            code_path=str(pipe_dir),
            config_path=config_path,
            doc_path=doc_path,
            mods=_discover_mods(pipe_dir),
            app_type=_detect_app_type(pipe_dir),
        )

    # Check subdirectories for additional flows
    for child in sorted(p for p in pipe_dir.iterdir() if p.is_dir()):
        if child.name.startswith("_") or child.name == "__pycache__":
            continue
        if (child / "Snakefile").exists() or (child / "config.yml").exists():
            flow = child.name
            key = f"{pipe}/{flow}" if flow != pipe else pipe
            if key in flows:
                continue
            doc_path = _find_doc_path(docs_dir, pipe, flow) if docs_dir else None
            config_path = _resolve_config_path(child)
            flows[key] = FlowEntry(
                name=flow,
                pipe=pipe,
                code_path=str(child),
                config_path=config_path,
                doc_path=doc_path,
                mods=_discover_mods(child),
                app_type=_detect_app_type(child),
            )

    # Check for .smk files at pipe root
    for smk in sorted(pipe_dir.glob("*.smk")):
        flow = smk.stem
        key = f"{pipe}/{flow}" if flow != pipe else pipe
        if key in flows:
            continue
        doc_path = _find_doc_path(docs_dir, pipe, flow) if docs_dir else None
        config_path = _resolve_config_path(pipe_dir)
        flows[key] = FlowEntry(
            name=flow,
            pipe=pipe,
            code_path=str(pipe_dir),
            config_path=config_path,
            doc_path=doc_path,
            app_type=_detect_app_type(pipe_dir),
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


def _find_doc_path(docs_dir: Path | None, pipe: str, flow: str) -> str | None:
    """Look for documentation for a pipe/flow in the docs directory."""
    if docs_dir is None or not docs_dir.exists():
        return None
    # Convention: docs/pipe-<pipe>/flow-<flow>/
    flow_doc = docs_dir / f"pipe-{pipe}" / f"flow-{flow}"
    if flow_doc.is_dir():
        return str(flow_doc)
    # Fallback: docs/pipe-<pipe>/ (when flow == pipe)
    if flow == pipe:
        pipe_doc = docs_dir / f"pipe-{pipe}"
        if pipe_doc.is_dir():
            return str(pipe_doc)
    return None
