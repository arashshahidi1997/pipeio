"""pipeio — Pipeline registry, notebook lifecycle, and flow management."""

from pipeio.config import FlowConfig, RegistryGroup, RegistryMember
from pipeio.contracts import Check, Contract, ContractResult
from pipeio.registry import (
    FlowEntry,
    ModEntry,
    PipelineRegistry,
    ValidationResult,
    slug_ok,
)
from pipeio.smk_log import setup_logging
from pipeio.resolver import (
    InputStage,
    PathResolver,
    PipelineContext,
    Session,
    SimpleResolver,
    Stage,
)
__all__ = [
    "BidsPaths",
    "Check",
    "Contract",
    "ContractResult",
    "FlowConfig",
    "FlowEntry",
    "InputStage",
    "ModEntry",
    "PathResolver",
    "PipelineContext",
    "PipelineRegistry",
    "RegistryGroup",
    "RegistryMember",
    "Session",
    "SimpleResolver",
    "Stage",
    "ValidationResult",
    "matlab2shell",
    "setup_logging",
    "slug_ok",
]


def __getattr__(name: str):
    if name == "BidsPaths":
        from pipeio.adapters.bids import BidsPaths

        return BidsPaths
    if name == "matlab2shell":
        from pipeio.matlab import matlab2shell

        return matlab2shell
    raise AttributeError(f"module 'pipeio' has no attribute {name!r}")
