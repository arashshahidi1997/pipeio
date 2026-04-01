"""Declarative input/output validation framework.

Contracts define expectations about pipeline inputs and outputs — file types,
required keys, shape constraints — that can be checked before or after a
pipeline run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


@dataclass
class Check:
    """A single validation check."""

    name: str
    description: str
    check_fn: Callable[[Path], bool]


@dataclass
class ContractResult:
    """Result of running a contract."""

    name: str
    passed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.failed) == 0 and len(self.errors) == 0


@dataclass
class Contract:
    """A named collection of checks applied to a set of paths."""

    name: str
    checks: list[Check] = field(default_factory=list)

    def validate(self, paths: list[Path]) -> ContractResult:
        """Run all checks against the provided paths."""
        result = ContractResult(name=self.name)
        for check in self.checks:
            for path in paths:
                try:
                    if check.check_fn(path):
                        result.passed.append(f"{check.name}: {path}")
                    else:
                        result.failed.append(f"{check.name}: {path}")
                except Exception as exc:
                    result.errors.append(f"{check.name}: {path}: {exc}")
        return result


# ---------------------------------------------------------------------------
# Registry-driven contract validation
# ---------------------------------------------------------------------------


@dataclass
class FlowValidation:
    """Result of validating a single flow's I/O contracts."""

    flow_id: str
    passed: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


def validate_flow_contracts(root: Path) -> list[FlowValidation]:
    """Validate I/O contracts for all flows in the registry.

    For each flow with a ``config.yml``, checks:

    1. **Config completeness** — ``input_dir`` and ``output_dir`` are set.
    2. **Input directory** — ``input_dir`` exists on disk.
    3. **Output directory** — ``output_dir`` exists on disk (warning if missing).
    4. **Registry groups** — each group has at least one member with
       non-empty suffix and extension.
    5. **Config validation** — runs ``FlowConfig.validate_config()``.
    """
    from pipeio.config import FlowConfig
    from pipeio.registry import PipelineRegistry

    # Find registry
    reg_path: Path | None = None
    for candidate in (
        root / ".projio" / "pipeio" / "registry.yml",
        root / ".pipeio" / "registry.yml",
    ):
        if candidate.exists():
            reg_path = candidate
            break

    if reg_path is None:
        return []

    registry = PipelineRegistry.from_yaml(reg_path)
    results: list[FlowValidation] = []

    for entry in registry.list_flows():
        fv = FlowValidation(flow_id=entry.name)

        if not entry.config_path:
            fv.warnings.append("No config_path in registry — skipping contract checks")
            results.append(fv)
            continue

        cfg_path = Path(entry.config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path

        if not cfg_path.exists():
            fv.errors.append(f"Config file not found: {entry.config_path}")
            results.append(fv)
            continue

        try:
            cfg = FlowConfig.from_yaml(cfg_path)
        except Exception as exc:
            fv.errors.append(f"Failed to parse config: {exc}")
            results.append(fv)
            continue

        # Run FlowConfig's own validation
        issues = cfg.validate_config()
        for issue in issues:
            fv.warnings.append(issue)

        # Check input_dir exists
        if cfg.input_dir:
            input_path = root / cfg.input_dir
            if input_path.exists():
                fv.passed.append(f"input_dir exists: {cfg.input_dir}")
            else:
                fv.warnings.append(f"input_dir not found: {cfg.input_dir}")

        # Check output_dir exists
        if cfg.output_dir:
            output_path = root / cfg.output_dir
            if output_path.exists():
                fv.passed.append(f"output_dir exists: {cfg.output_dir}")
            else:
                fv.warnings.append(f"output_dir not found: {cfg.output_dir}")

        # Validate registry groups have proper members
        for group_name, group in cfg.registry.items():
            if not group.members:
                fv.errors.append(f"Group {group_name!r} has no members")
            else:
                fv.passed.append(
                    f"Group {group_name!r}: {len(group.members)} member(s)"
                )
            for member_name, member in group.members.items():
                if not member.suffix or not member.extension:
                    fv.errors.append(
                        f"Member {member_name!r} in {group_name!r}: "
                        f"missing suffix or extension"
                    )

        results.append(fv)

    return results
