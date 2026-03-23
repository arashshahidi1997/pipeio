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
