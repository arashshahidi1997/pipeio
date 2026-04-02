"""Flow configuration loading and validation.

Loads and validates the per-flow ``config.yml`` that declares inputs, outputs,
and the output registry (the declarative data contract consumed by both
workflow engines and notebooks).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class RegistryMember(BaseModel):
    """A single output product within a registry group."""

    suffix: str
    extension: str


class RegistryGroup(BaseModel):
    """A group of related outputs (a pipeline stage / family)."""

    base_input: str | None = None
    bids: dict[str, str] = Field(default_factory=dict)
    members: dict[str, RegistryMember] = Field(default_factory=dict)


class FlowConfig(BaseModel):
    """Schema for a flow's ``config.yml``."""

    input_dir: str = ""
    input_manifest: str = ""
    output_dir: str = ""
    output_manifest: str = ""
    registry: dict[str, RegistryGroup] = Field(default_factory=dict)

    # Pass-through for workflow-engine-specific fields (pybids_inputs, etc.)
    extra: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: Path) -> FlowConfig:
        """Load a FlowConfig from a YAML file."""
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}
        known = {
            "input_dir",
            "input_manifest",
            "output_dir",
            "output_manifest",
            "registry",
        }
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(**{k: v for k, v in raw.items() if k in known}, extra=extra)

    def extra_inputs(self) -> dict[str, tuple[str, str]]:
        """Discover secondary input sources from extra config keys.

        Returns a mapping of ``{name: (input_dir, input_manifest)}`` for each
        ``input_dir_<name>`` / ``input_manifest_<name>`` pair found in extra.
        """
        result: dict[str, tuple[str, str]] = {}
        for key, val in sorted(self.extra.items()):
            if not (isinstance(key, str) and key.startswith("input_dir_")):
                continue
            suffix = key.removeprefix("input_dir_")
            manifest_key = f"input_manifest_{suffix}"
            if manifest_key in self.extra:
                result[suffix] = (str(val), str(self.extra[manifest_key]))
        return result

    def groups(self) -> list[str]:
        """Return sorted registry group names."""
        return sorted(self.registry.keys())

    def products(self, group: str) -> list[str]:
        """Return member names for a registry group."""
        if group not in self.registry:
            known = ", ".join(self.groups()) or "(none)"
            raise KeyError(f"Unknown group: {group!r}. Known: {known}")
        return list(self.registry[group].members.keys())

    def validate_config(self) -> list[str]:
        """Validate the config and return a list of error/warning messages.

        Returns an empty list if everything is valid.
        """
        issues: list[str] = []

        if not self.input_dir:
            issues.append("input_dir is empty")
        if not self.output_dir:
            issues.append("output_dir is empty")

        for group_name, group in self.registry.items():
            if not group.members:
                issues.append(f"Registry group {group_name!r} has no members")
            for member_name, member in group.members.items():
                if not member.suffix:
                    issues.append(
                        f"Member {member_name!r} in group {group_name!r} "
                        f"has empty suffix"
                    )
                if not member.extension:
                    issues.append(
                        f"Member {member_name!r} in group {group_name!r} "
                        f"has empty extension"
                    )

        # Warn on duplicate member names across groups
        seen_members: dict[str, str] = {}
        for group_name, group in self.registry.items():
            for member_name in group.members:
                if member_name in seen_members:
                    issues.append(
                        f"Duplicate member name {member_name!r} in groups "
                        f"{seen_members[member_name]!r} and {group_name!r}"
                    )
                else:
                    seen_members[member_name] = group_name

        return issues
