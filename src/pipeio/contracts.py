"""Declarative input/output validation framework.

Contracts define expectations about pipeline inputs and outputs — file types,
required keys, shape constraints — that can be checked before or after a
pipeline run.
"""

from __future__ import annotations

import importlib.util
import inspect
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
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


def import_flow_module(flow_dir: Path, module_name: str) -> ModuleType | None:
    """Import a module from a flow directory without mutating sys.path.

    Returns ``None`` if the module file does not exist.  Raises on import
    errors (e.g. missing dependencies) so the caller can report them.
    """
    module_path = flow_dir / f"{module_name}.py"
    if not module_path.exists():
        return None
    spec = importlib.util.spec_from_file_location(
        f"pipeio._flow_modules.{flow_dir.name}.{module_name}",
        module_path,
    )
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@dataclass
class FlowValidation:
    """Result of validating a single flow's I/O contracts."""

    flow_id: str
    passed: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    has_contracts: bool = False
    contract_functions: list[str] = field(default_factory=list)
    contract_results: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


_CONTRACT_FUNCTIONS = ("validate_inputs", "validate_outputs")


def _discover_contracts(flow_dir: Path, fv: FlowValidation) -> ModuleType | None:
    """Try to import contracts.py from *flow_dir* and populate *fv* fields.

    Returns the imported module on success, ``None`` otherwise.
    """
    try:
        mod = import_flow_module(flow_dir, "contracts")
    except Exception as exc:
        fv.warnings.append(f"contracts.py exists but failed to import: {exc}")
        fv.has_contracts = True
        return None

    if mod is None:
        return None

    fv.has_contracts = True
    for fn_name in _CONTRACT_FUNCTIONS:
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            fv.contract_functions.append(fn_name)
    if not fv.contract_functions:
        fv.warnings.append(
            "contracts.py found but contains no validate_inputs/validate_outputs"
        )
    else:
        fv.passed.append(
            f"contracts.py: {', '.join(fv.contract_functions)}"
        )
    return mod


def _run_contract_function(
    mod: ModuleType,
    fn_name: str,
    kwargs: dict[str, Any],
    fv: FlowValidation,
) -> None:
    """Execute a single contract function and record results in *fv*."""
    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        return

    # Only pass kwargs that the function actually accepts.
    sig = inspect.signature(fn)
    accepted = {
        k: v for k, v in kwargs.items() if k in sig.parameters
    }
    missing = [
        p.name
        for p in sig.parameters.values()
        if p.default is inspect.Parameter.empty and p.name not in accepted
    ]
    if missing:
        fv.contract_results[fn_name] = {
            "status": "skipped",
            "reason": f"missing required arguments: {missing}",
        }
        return

    try:
        info = fn(**accepted)
        fv.contract_results[fn_name] = {"status": "passed", "info": info}
        fv.passed.append(f"{fn_name}: passed")
    except Exception as exc:
        fv.contract_results[fn_name] = {
            "status": "failed",
            "error": str(exc),
        }
        fv.errors.append(f"{fn_name}: {exc}")


def validate_flow_contracts(
    root: Path,
    *,
    run: bool = False,
    run_kwargs: dict[str, dict[str, Any]] | None = None,
) -> list[FlowValidation]:
    """Validate I/O contracts for all flows in the registry.

    For each flow with a ``config.yml``, checks:

    1. **Config completeness** — ``input_dir`` and ``output_dir`` are set.
    2. **Input directory** — ``input_dir`` exists on disk.
    3. **Output directory** — ``output_dir`` exists on disk (warning if missing).
    4. **Registry groups** — each group has at least one member with
       non-empty suffix and extension.
    5. **Config validation** — runs ``FlowConfig.validate_config()``.
    6. **Contract discovery** — imports ``contracts.py`` and introspects
       ``validate_inputs`` / ``validate_outputs``.
    7. **Contract execution** (when *run=True*) — calls discovered functions
       with keyword arguments from *run_kwargs*.

    Parameters
    ----------
    root:
        Project root directory.
    run:
        If ``True``, execute discovered contract functions.
    run_kwargs:
        Per-function keyword arguments for execution, keyed by function
        name (``"validate_inputs"`` / ``"validate_outputs"``).
        Values must be ``Path`` objects for file arguments.
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

        # Check bids_dir exists (when set; defaults to input_dir otherwise)
        if cfg.bids_dir:
            bids_path = root / cfg.bids_dir
            if bids_path.exists():
                fv.passed.append(f"bids_dir exists: {cfg.bids_dir}")
            else:
                fv.warnings.append(f"bids_dir not found: {cfg.bids_dir}")

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

        # --- Contract discovery (step 6) ---
        flow_dir = Path(entry.code_path)
        if not flow_dir.is_absolute():
            flow_dir = root / flow_dir
        contracts_mod = _discover_contracts(flow_dir, fv)

        # --- Contract execution (step 7, only when run=True) ---
        if run and contracts_mod is not None and fv.contract_functions:
            kw = run_kwargs or {}
            for fn_name in fv.contract_functions:
                fn_kw = kw.get(fn_name, {})
                _run_contract_function(contracts_mod, fn_name, fn_kw, fv)

        results.append(fv)

    return results
