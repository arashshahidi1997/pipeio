"""Path resolution protocol and context.

Defines the ``PathResolver`` protocol that adapters (e.g. BIDS/snakebids)
implement.  ``PipelineContext`` and ``Session`` use the resolver to translate
(group, member, entities) tuples into concrete filesystem paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, Sequence, runtime_checkable

from pipeio.config import FlowConfig


@runtime_checkable
class PathResolver(Protocol):
    """Protocol for resolving output paths from registry entries."""

    def resolve(self, group: str, member: str, **entities: str) -> Path:
        """Return the concrete path for a single (group, member, entities) tuple."""
        ...

    def expand(
        self, group: str, member: str, **filters: str
    ) -> list[Path]:
        """Enumerate all paths matching a (group, member) with optional entity filters."""
        ...


class SimpleResolver:
    """Non-BIDS path resolver using simple string template expansion.

    Constructs paths as::

        {root}/{group_root}/{entity_dirs}/{suffix}{extension}

    where ``group_root`` comes from ``bids.root`` in the registry group,
    and ``entity_dirs`` are constructed from entity key-value pairs.
    """

    def __init__(self, config: FlowConfig, root: Path) -> None:
        self._config = config
        self._root = root

    def resolve(self, group: str, member: str, **entities: str) -> Path:
        """Resolve a single path from the registry group and member."""
        grp = self._config.registry.get(group)
        if grp is None:
            raise KeyError(f"Unknown group: {group!r}")
        mem = grp.members.get(member)
        if mem is None:
            known = ", ".join(grp.members.keys()) or "(none)"
            raise KeyError(f"Unknown member: {member!r} in group {group!r}. Known: {known}")

        group_root = grp.bids.get("root", group)
        output_dir = self._config.output_dir

        # Build entity directory segments (e.g. sub-01/ses-pre)
        entity_parts = [f"{k}-{v}" for k, v in sorted(entities.items())]
        entity_path = "/".join(entity_parts) if entity_parts else ""

        # Build filename from entities + suffix + extension
        entity_prefix = "_".join(entity_parts)
        suffix_part = f"_suffix-{mem.suffix}" if mem.suffix else ""
        filename = f"{entity_prefix}{suffix_part}{mem.extension}" if entity_prefix else f"suffix-{mem.suffix}{mem.extension}"

        parts = [output_dir, group_root]
        if entity_path:
            parts.append(entity_path)
        parts.append(filename)

        return self._root / "/".join(parts)

    def expand(self, group: str, member: str, **filters: str) -> list[Path]:
        """Enumerate matching paths by globbing the filesystem."""
        grp = self._config.registry.get(group)
        if grp is None:
            raise KeyError(f"Unknown group: {group!r}")
        mem = grp.members.get(member)
        if mem is None:
            raise KeyError(f"Unknown member: {member!r} in group {group!r}")

        group_root = grp.bids.get("root", group)
        output_dir = self._config.output_dir
        base = self._root / output_dir / group_root

        if not base.exists():
            return []

        # Glob for files matching the suffix+extension pattern
        pattern = f"**/*suffix-{mem.suffix}{mem.extension}"
        matches = sorted(base.glob(pattern))

        # Apply entity filters
        if filters:
            filtered = []
            for p in matches:
                path_str = str(p)
                if all(f"{k}-{v}" in path_str for k, v in filters.items()):
                    filtered.append(p)
            matches = filtered

        return matches


@dataclass(frozen=True, slots=True)
class Stage:
    """Handle to a single output-registry group (stage).

    Attributes
    ----------
    ctx : PipelineContext
        Back-reference to the owning context.
    name : str
        Resolved registry group name (after alias expansion).
    """

    ctx: PipelineContext
    name: str

    def paths(
        self,
        sess: Session,
        *,
        members: Sequence[str] | None = None,
    ) -> dict[str, Path]:
        """Return ``{member: Path}`` for this stage's products."""
        all_members = self.ctx.products(self.name)
        requested = list(members) if members is not None else all_members
        missing = [m for m in requested if m not in all_members]
        if missing:
            raise KeyError(
                f"Stage {self.name!r} has no member(s): "
                f"{', '.join(map(repr, missing))}. "
                f"Known members: {', '.join(map(repr, all_members))}"
            )
        return {m: sess.get(self.name, m) for m in requested}

    def have(
        self,
        sess: Session,
        *,
        members: Sequence[str] | None = None,
    ) -> bool:
        """Return ``True`` if every requested member exists on disk."""
        return all(p.exists() for p in self.paths(sess, members=members).values())

    def resolve(
        self,
        sess: Session,
        prefer: Sequence[str],
    ) -> str:
        """Return the first stage name in *prefer* whose files exist on disk."""
        for candidate in prefer:
            try:
                if self.ctx.stage(candidate).have(sess):
                    return candidate
            except KeyError:
                continue
        raise FileNotFoundError(
            f"No preferred stages exist on disk for session "
            f"{sess.entities}. Tried: {', '.join(map(repr, prefer))}"
        )


@dataclass(frozen=True, slots=True)
class Session:
    """A session binds wildcard entities, then resolves paths via the resolver."""

    resolver: PathResolver
    entities: dict[str, str] = field(default_factory=dict)
    _ctx: PipelineContext | None = field(default=None, repr=False)

    def get(self, group: str, member: str, **overrides: str) -> Path:
        """Resolve a single path with this session's bound entities."""
        merged = dict(self.entities)
        merged.update({k: v for k, v in overrides.items() if v is not None})
        return self.resolver.resolve(group, member, **merged)

    def have(self, group: str, member: str, **overrides: str) -> bool:
        """Check whether the resolved path exists."""
        return self.get(group, member, **overrides).exists()

    def bundle(self, group: str) -> dict[str, Path]:
        """Return ``{member: Path}`` for all members of a registry group."""
        if self._ctx is None:
            raise ValueError("bundle() requires a PipelineContext-created session")
        members = self._ctx.products(group)
        return {m: self.get(group, m) for m in members}


@dataclass(frozen=True, slots=True)
class PipelineContext:
    """Registry-driven pipeline context with path resolution."""

    root: Path
    resolver: PathResolver
    config: FlowConfig = field(default_factory=FlowConfig)
    meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_config(cls, flow_config: FlowConfig, root: Path) -> PipelineContext:
        """Create a PipelineContext from a loaded FlowConfig using SimpleResolver."""
        resolver = SimpleResolver(flow_config, root)
        return cls(root=root, resolver=resolver, config=flow_config)

    @classmethod
    def from_registry(
        cls,
        flow: str,
        *,
        root: Path,
        registry: Any | None = None,
    ) -> PipelineContext:
        """Create a PipelineContext by looking up a flow in the pipeline registry."""
        from pipeio.registry import PipelineRegistry

        if registry is None:
            reg_path = root / ".pipeio" / "registry.yml"
            if not reg_path.exists():
                raise FileNotFoundError(
                    f"No pipeline registry at {reg_path}. Run 'pipeio init'."
                )
            registry = PipelineRegistry.from_yaml(reg_path)

        entry = registry.get(flow)

        config_path = entry.config_path
        if config_path is None:
            raise FileNotFoundError(
                f"No config_path for flow {entry.name}"
            )

        cfg_path = Path(config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path

        flow_config = FlowConfig.from_yaml(cfg_path)
        resolver = SimpleResolver(flow_config, root)
        return cls(
            root=root,
            resolver=resolver,
            config=flow_config,
            meta={"flow": entry.name},
        )

    def session(self, **entities: str) -> Session:
        """Create a session with bound entities."""
        return Session(resolver=self.resolver, entities=entities, _ctx=self)

    def groups(self) -> list[str]:
        """Return registry group names."""
        return self.config.groups()

    def products(self, group: str) -> list[str]:
        """Return member names for a registry group."""
        return self.config.products(group)

    def pattern(self, group: str, member: str) -> str:
        """Return the path template with ``{entity}`` placeholders."""
        grp = self.config.registry.get(group)
        if grp is None:
            raise KeyError(f"Unknown group: {group!r}")
        mem = grp.members.get(member)
        if mem is None:
            raise KeyError(f"Unknown member: {member!r} in group {group!r}")
        group_root = grp.bids.get("root", group)
        return (
            f"{self.config.output_dir}/{group_root}/"
            f"{{entities}}/suffix-{mem.suffix}{mem.extension}"
        )

    def path(self, group: str, member: str, **entities: str) -> Path:
        """Resolve a single path."""
        return self.resolver.resolve(group, member, **entities)

    def have(self, group: str, member: str, **entities: str) -> bool:
        """Check whether a resolved path exists."""
        return self.path(group, member, **entities).exists()

    def expand(
        self,
        group: str,
        member: str,
        **filters: str,
    ) -> list[Path]:
        """Enumerate all matching paths with optional filters."""
        return self.resolver.expand(group, member, **filters)

    def stage(self, name: str) -> Stage:
        """Return a Stage handle for an output-registry group.

        *name* may be a literal registry group key or a config-driven alias
        defined under ``stage_aliases`` in the extra config.
        """
        aliases = self.config.extra.get("stage_aliases") or {}
        resolved = aliases.get(name, name)
        groups = self.groups()
        if resolved not in groups:
            tag = f" (alias for {resolved!r})" if resolved != name else ""
            raise KeyError(
                f"Unknown stage: {name!r}{tag}. "
                f"Known groups: {', '.join(map(repr, groups))}"
            )
        return Stage(ctx=self, name=resolved)
