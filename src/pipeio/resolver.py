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
    """BIDS path resolver that builds the same paths as the flow's ``BidsPaths``.

    Paths follow ``snakebids.bids()`` under the flow's bids spec::

        {output_dir}/{bids.root}/sub-{subject}/ses-{session}/{datatype}/
          sub-{subject}_ses-{session}_task-{task}_..._{suffix}{extension}

    The registry family is merged by :func:`pipeio.bidspath.family_kwargs`
    (the code ``BidsPaths`` runs), with base-input wildcards read from the
    config's ``pybids_inputs`` instead of scanning the dataset.

    Args:
        config: The flow config.
        root: Project root.
        spec: snakebids spec name (``v0_0_0``, ``v0_11_0``, ``latest``, ...);
            detect it with :func:`pipeio.bidspath.flow_bids_spec`.
        registry: Raw registry dict to resolve against instead of
            ``config.registry`` (e.g. one read from the evaluated Snakefile,
            which may add families at runtime).
        base_wildcards: ``{input: {wildcard: template}}``; defaults to the
            config's ``pybids_inputs``.
        out_root: Directory ``bids.root`` is joined to; defaults to
            ``root / config.output_dir``.
    """

    def __init__(
        self,
        config: FlowConfig,
        root: Path,
        *,
        spec: str = "latest",
        registry: dict[str, Any] | None = None,
        base_wildcards: dict[str, dict[str, str]] | None = None,
        out_root: Path | None = None,
    ) -> None:
        from pipeio.bidspath import config_base_wildcards

        self._config = config
        self._root = root
        self._spec = spec
        self._registry = registry if registry is not None else config.raw_registry()
        self._base_wildcards = (
            base_wildcards if base_wildcards is not None
            else config_base_wildcards(config.extra)
        )
        self._out_root = out_root if out_root is not None else root / config.output_dir

    @property
    def registry(self) -> dict[str, Any]:
        """The raw registry this resolver resolves against."""
        return self._registry

    def _kwargs(self, group: str, member: str, entities: dict[str, Any]) -> dict[str, Any]:
        from pipeio.bidspath import align_entities, family_kwargs

        if group not in self._registry:
            raise KeyError(f"Unknown group: {group!r}")
        members = self._registry[group].get("members") or {}
        if member not in members:
            known = ", ".join(members) or "(none)"
            raise KeyError(f"Unknown member: {member!r} in group {group!r}. Known: {known}")
        # A base input the config does not declare (added in the Snakefile)
        # contributes no wildcards; the caller supplies the entities.
        base = self._registry[group].get("base_input")
        wcs = dict(self._base_wildcards)
        if base and base not in wcs:
            wcs[base] = {}
        template = family_kwargs(self._registry, group, member, base_wildcards=wcs)
        ents = align_entities(entities, template, self._spec)
        return family_kwargs(self._registry, group, member, ents, base_wildcards=wcs)

    def resolve(self, group: str, member: str, **entities: str) -> Path:
        """Resolve a single path from the registry group and member."""
        from pipeio.bidspath import bids_path

        kwargs = self._kwargs(group, member, entities)
        root_rel = kwargs.pop("root", "")
        out_root = self._out_root / root_rel if root_rel else self._out_root
        return Path(bids_path(out_root, spec=self._spec, **kwargs))

    def pattern(self, group: str, member: str) -> str:
        """Path template with ``{wildcard}`` placeholders, relative to the project root."""
        path = self.resolve(group, member)
        try:
            return str(path.relative_to(self._root))
        except ValueError:
            return str(path)

    def expand(self, group: str, member: str, **filters: str) -> list[Path]:
        """Enumerate matching paths by globbing the filesystem."""
        from pipeio.bidspath import entity_tags

        kwargs = self._kwargs(group, member, {})
        root_rel = kwargs.get("root", "")
        base = self._out_root / root_rel if root_rel else self._out_root
        if not base.exists():
            return []

        suffix = kwargs.get("suffix") or ""
        extension = kwargs.get("extension") or ""
        matches = sorted(base.glob(f"**/*{suffix}{extension}"))

        if filters:
            tags = {**_BIDS_ABBREV, **entity_tags(self._spec)}
            matches = [
                p for p in matches
                if all(f"{tags.get(k, k)}-{v}" in str(p) for k, v in filters.items())
            ]
        return matches


def _resolve_stage(ctx: PipelineContext, sess: Session, prefer: Sequence[str]) -> str:
    """Return the first stage name in *prefer* whose files exist on disk."""
    for candidate in prefer:
        try:
            stage = ctx.stage(candidate)
            if isinstance(stage, InputStage):
                # For input stages, check each member individually —
                # at least one must exist (not all pybids_inputs may apply).
                if any(
                    stage._resolve_path(sess, m).exists()
                    for m in stage.members()
                ):
                    return candidate
            else:
                if stage.have(sess):
                    return candidate
        except KeyError:
            continue
    raise FileNotFoundError(
        f"No preferred stages exist on disk for session "
        f"{sess.entities}. Tried: {', '.join(map(repr, prefer))}"
    )


# BIDS entity key → filename prefix abbreviation
_BIDS_ABBREV: dict[str, str] = {
    "subject": "sub",
    "session": "ses",
    "task": "task",
    "acquisition": "acq",
    "recording": "rec",
    "run": "run",
    "space": "space",
    "description": "desc",
}

# Canonical ordering of BIDS entity keys in filenames
_BIDS_ENTITY_ORDER: tuple[str, ...] = (
    "subject", "session", "task", "acquisition",
    "recording", "run", "space", "description",
)


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
        return _resolve_stage(self.ctx, sess, prefer)


@dataclass(frozen=True, slots=True)
class InputStage:
    """Handle to a virtual input stage derived from ``pybids_inputs``.

    Members correspond to ``pybids_inputs`` keys and paths are constructed
    using BIDS conventions under the flow's ``input_dir``.
    """

    ctx: PipelineContext
    name: str
    _pybids_inputs: dict[str, Any] = field(repr=False)

    def members(self) -> list[str]:
        """Return sorted member (pybids_inputs key) names."""
        return sorted(self._pybids_inputs.keys())

    def _resolve_path(self, sess: Session, member: str) -> Path:
        """Construct a BIDS-style input path for *member*."""
        spec = self._pybids_inputs[member]
        filters = spec.get("filters", {})
        entities = sess.entities
        suffix = filters.get("suffix", member)
        extension = filters.get("extension", "")
        datatype = filters.get("datatype", "")

        # Directory: {input_dir}/sub-{subject}/ses-{session}/{datatype}/
        parts: list[str] = [self.ctx.config.input_dir]
        if "subject" in entities:
            parts.append(f"sub-{entities['subject']}")
        if "session" in entities:
            parts.append(f"ses-{entities['session']}")
        if datatype:
            parts.append(datatype)

        # Filename: sub-XX_ses-XX_task-XX_..._suffix.ext
        name_parts: list[str] = []
        for key in _BIDS_ENTITY_ORDER:
            if key in entities:
                abbrev = _BIDS_ABBREV.get(key, key)
                name_parts.append(f"{abbrev}-{entities[key]}")
        name_parts.append(suffix)
        filename = "_".join(name_parts) + extension

        parts.append(filename)
        return self.ctx.root / "/".join(parts)

    def paths(
        self,
        sess: Session,
        *,
        members: Sequence[str] | None = None,
    ) -> dict[str, Path]:
        """Return ``{member: Path}`` for input files."""
        all_members = self.members()
        requested = list(members) if members is not None else all_members
        missing = [m for m in requested if m not in all_members]
        if missing:
            raise KeyError(
                f"Input stage {self.name!r} has no member(s): "
                f"{', '.join(map(repr, missing))}. "
                f"Known members: {', '.join(map(repr, all_members))}"
            )
        return {m: self._resolve_path(sess, m) for m in requested}

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
        return _resolve_stage(self.ctx, sess, prefer)


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
        from pipeio.registry import PipelineRegistry, find_registry

        if registry is None:
            reg_path = find_registry(root)
            if reg_path is None:
                raise FileNotFoundError(
                    f"No pipeline registry found under {root}. Run 'pipeio init'."
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

        from pipeio.bidspath import flow_bids_spec

        flow_config = FlowConfig.from_yaml(cfg_path)
        resolver = SimpleResolver(
            flow_config, root, spec=flow_bids_spec(cfg_path.parent),
        )
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
        if hasattr(self.resolver, "pattern"):
            return self.resolver.pattern(group, member)
        grp = self.config.registry.get(group)
        if grp is None:
            raise KeyError(f"Unknown group: {group!r}")
        mem = grp.members.get(member)
        if mem is None:
            raise KeyError(f"Unknown member: {member!r} in group {group!r}")
        group_root = grp.bids.get("root", group)
        return (
            f"{self.config.output_dir}/{group_root}/"
            f"{{entities}}/{mem.suffix}{mem.extension}"
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

    def input_stages(self) -> list[str]:
        """Return virtual input stage names derived from ``input_dir``.

        If the config has ``pybids_inputs`` in its extra fields and a
        non-empty ``input_dir``, the ``input_dir`` value is exposed as a
        stage name whose members are the ``pybids_inputs`` keys.
        """
        pybids = self.config.extra.get("pybids_inputs")
        if pybids and self.config.input_dir:
            return [self.config.input_dir]
        return []

    def stage(self, name: str) -> Stage | InputStage:
        """Return a Stage handle for an output-registry group or input stage.

        Resolution order:

        1. Expand ``stage_aliases`` from extra config.
        2. Check output registry groups → returns :class:`Stage`.
        3. Check input stages (``input_dir`` with ``pybids_inputs``) →
           returns :class:`InputStage`.

        Raises :class:`KeyError` if the name is not found in any tier.
        """
        aliases = self.config.extra.get("stage_aliases") or {}
        resolved = aliases.get(name, name)

        # Output registry groups
        groups = self.groups()
        if resolved in groups:
            return Stage(ctx=self, name=resolved)

        # Input stages (virtual, from pybids_inputs + input_dir)
        pybids = self.config.extra.get("pybids_inputs")
        if pybids and resolved == self.config.input_dir:
            return InputStage(ctx=self, name=resolved, _pybids_inputs=pybids)

        tag = f" (alias for {resolved!r})" if resolved != name else ""
        known = list(map(repr, groups)) + list(map(repr, self.input_stages()))
        raise KeyError(
            f"Unknown stage: {name!r}{tag}. "
            f"Known stages: {', '.join(known)}"
        )
