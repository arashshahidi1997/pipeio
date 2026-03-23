# Path resolution design

## The problem

Pipeline code needs to locate output files — `Where is the badlabel .npy for subject 01, session pre?` Hard-coding paths is fragile. Different projects use different directory conventions (BIDS, flat, custom).

## The solution: PathResolver protocol

Pipeio defines a **protocol** (interface) that any resolver can implement:

```python
class PathResolver(Protocol):
    def resolve(self, group: str, member: str, **entities: str) -> Path: ...
    def expand(self, group: str, member: str, **filters: str) -> list[Path]: ...
```

- `resolve()` returns one concrete path given a group, member, and entity values
- `expand()` enumerates all matching paths (useful for batch processing)

## Adapters

### SimpleResolver (built-in)

The default adapter constructs paths from the config registry using string templates:

```
{output_dir}/{group_root}/{entity_dirs}/{entity_prefix}_suffix-{suffix}{extension}
```

No external dependencies required.

### BidsResolver (optional)

The `pipeio[bids]` adapter uses snakebids to construct BIDS-compliant paths. It requires `snakebids>=0.15` and is designed for neuroimaging projects.

## Layered API

```
PipelineContext    ← high-level: from_config(), from_registry()
    ↓
Session            ← binds entities, delegates to resolver
    ↓
PathResolver       ← protocol: resolve(), expand()
    ↓
SimpleResolver     ← concrete: string template paths
BidsResolver       ← concrete: snakebids BIDS paths
```

## PipelineContext

The entry point for notebooks and scripts:

- `from_config(flow_config, root)` — create from a loaded FlowConfig
- `from_registry(pipe, flow, root)` — create by looking up the pipeline registry
- `groups()`, `products(group)` — query the output registry
- `path()`, `have()`, `expand()` — resolve paths directly
- `session(**entities)` — create a Session with bound entities
- `stage(name)` — get a Stage handle for existence checks and fallback

## Session

Binds entity values once so every subsequent call is concise:

```python
sess = ctx.session(subject="01", session="pre")
sess.get("badlabel", "npy")      # entities already bound
sess.bundle("badlabel")          # all members at once
```

## Stage

A handle to one output-registry group, providing:

- `paths(sess)` — all member paths for a session
- `have(sess)` — do all members exist?
- `resolve(sess, prefer=[...])` — multi-stage fallback

## Design decisions

- **Protocol, not base class** — adapters don't inherit from a common class; they just implement the two methods. This keeps the core dependency-free.
- **Frozen dataclasses** — PipelineContext, Session, and Stage are immutable, matching the pixecog pattern.
- **Config-driven** — the output registry in config.yml is the single source of truth for what a pipeline produces.
