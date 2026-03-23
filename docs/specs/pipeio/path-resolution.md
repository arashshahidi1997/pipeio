# pipeio: Path Resolution Specification

## Purpose

Path resolution translates abstract references `(group, member, entities)` into concrete filesystem paths. This is the core API that notebooks and scripts use to locate pipeline inputs and outputs without hard-coding paths.

## Design: Protocol + Adapters

The key abstraction is `PathResolver` — a runtime-checkable protocol that adapters implement. The core classes (`PipelineContext`, `Session`) use this protocol, making them workflow-engine-agnostic.

```python
@runtime_checkable
class PathResolver(Protocol):
    def resolve(self, group: str, member: str, **entities: str) -> Path:
        """Resolve a single path for (group, member, entities)."""
        ...

    def expand(self, group: str, member: str, **filters: str) -> list[Path]:
        """Enumerate all paths matching (group, member) with optional filters."""
        ...
```

## Reference: pixecog's BidsPaths Pattern

In pixecog, path resolution is backed by `sutil.bids.paths.BidsPaths` + `snakebids.generate_inputs()`:

```python
# pixecog/code/utils/io/context.py (simplified)
from snakebids import generate_inputs
from sutil.bids.paths import BidsPaths

inputs = generate_inputs(input_dir, cfg["pybids_inputs"])
out_paths = BidsPaths(cfg["registry"], output_dir, inputs)

# Resolve: out_paths("badlabel", "npy", subject="test", session="01", task="pre")
# → Path("derivatives/preprocess/badlabel/sub-test/ses-01/sub-test_ses-01_task-pre_suffix-ieeg.npy")
```

`BidsPaths` interprets the registry group's `bids` section to construct BIDS-compliant path templates, then substitutes entity values. `generate_inputs()` discovers which entity combinations exist on disk.

## PipelineContext

The main entry point for notebooks and scripts:

```python
@dataclass(frozen=True, slots=True)
class PipelineContext:
    root: Path                    # project root
    resolver: PathResolver        # injected adapter
    meta: dict[str, Any] = {}    # flow metadata (pipe, flow, config)

    def session(self, **entities: str) -> Session:
        """Bind entities to create a session handle."""
        return Session(resolver=self.resolver, entities=entities)
```

### Construction

Two paths to create a `PipelineContext`:

**1. From registry (high-level):**
```python
ctx = PipelineContext.from_registry(
    "preprocess", flow="ieeg",
    root=project_root,
    adapter="bids",               # selects BidsResolver
)
```

**2. From explicit resolver (low-level):**
```python
from pipeio.adapters.bids import BidsResolver

resolver = BidsResolver(config_path=Path("code/pipelines/preprocess/ieeg/config.yml"))
ctx = PipelineContext(root=project_root, resolver=resolver)
```

### Core API

```python
# Groups and members
ctx.groups()                      # → ['raw_zarr', 'badlabel', 'filter', ...]
ctx.products("badlabel")          # → ['npy', 'featuremap']

# Path resolution (requires entities)
ctx.path("badlabel", "npy", subject="test", session="01", task="pre")
ctx.have("badlabel", "npy", subject="test", session="01", task="pre")

# Pattern (with wildcards)
ctx.pattern("badlabel", "npy")    # → template string with {subject}, {session}, etc.

# Expansion (enumerate all matching sessions)
ctx.expand("badlabel", "npy", filter={"task": "pre"}, max_n=10)
```

## Session

Binds entity values so every subsequent call doesn't repeat them:

```python
sess = ctx.session(subject="test", session="01", task="pre")

# Now just (group, member) — entities are bound
path = sess.get("badlabel", "npy")
exists = sess.have("badlabel", "npy")

# Override a bound entity
path = sess.get("badlabel", "npy", task="post")

# Bundle: all members of a group at once
paths = sess.bundle("badlabel")   # → {'npy': Path(...), 'featuremap': Path(...)}
```

### Reference: pixecog Session

```python
# pixecog/code/utils/io/session.py
@dataclass(frozen=True, slots=True)
class Session:
    ctx: Any                      # PipelineContext
    entities: dict[str, str]

    def get(self, group, product, **overrides):
        merged = {**self.entities, **{k:v for k,v in overrides.items() if v}}
        return self.ctx.path(group, product, **merged)

    def have(self, group, product, **overrides):
        return self.get(group, product, **overrides).exists()

    def bundle(self, group, bundle, **overrides):
        members = self.ctx.bundle_members(bundle=bundle, group=group)
        return {m: self.ctx.path(group, m, **merged) for m in members}
```

## Stage

A handle to a single output-registry group, providing existence checks and multi-stage fallback resolution:

```python
stage = ctx.stage("badlabel")

# All paths for a session
stage.paths(sess)                 # → {'npy': Path(...), 'featuremap': Path(...)}

# Existence check
stage.have(sess)                  # → True if all members exist

# Multi-stage resolution: try stages in order, return first that exists
stage.resolve(sess, prefer=["interpolate", "filter", "raw_zarr"])
# → "interpolate" if its files exist, else "filter", else "raw_zarr"
```

### Reference: pixecog Stage

```python
# pixecog/code/utils/io/stage.py
@dataclass(frozen=True, slots=True)
class Stage:
    ctx: PipelineContext
    name: str

    def paths(self, sess, *, members=None):
        all_members = self.ctx.products(self.name)
        requested = list(members) if members else all_members
        return {m: sess.get(self.name, m) for m in requested}

    def have(self, sess, *, members=None):
        return all(p.exists() for p in self.paths(sess, members=members).values())

    def resolve(self, sess, prefer):
        for candidate in prefer:
            if self.ctx.stage(candidate).have(sess):
                return candidate
        raise FileNotFoundError(...)
```

## BidsResolver Adapter

The BIDS adapter (`pipeio[bids]`) implements `PathResolver` using snakebids:

```python
class BidsResolver:
    def __init__(self, config_path: Path, root: Path | None = None):
        from snakebids import generate_inputs
        from sutil.bids.paths import BidsPaths  # or bundled equivalent

        cfg = yaml.safe_load(config_path.read_text())
        self._root = root or config_path.parent
        self._inputs = generate_inputs(cfg["input_dir"], cfg["pybids_inputs"])
        self._out_paths = BidsPaths(cfg["registry"], cfg["output_dir"], self._inputs)

    def resolve(self, group, member, **entities):
        return Path(self._out_paths(group, member, **entities))

    def expand(self, group, member, **filters):
        comp = self._inputs[self._out_paths.registry[group].get("base_input", ...)]
        if filters:
            comp = comp.filter(**filters)
        return [Path(p) for p in comp.expand(self._out_paths(group, member))]
```

## Generalization Notes

The pixecog implementation has these project-specific aspects that pipeio generalizes:

| pixecog | pipeio |
|---------|--------|
| `REPO_ROOT = Path(__file__).resolve().parents[3]` | Explicit `root` parameter |
| Hard import of `snakebids` | Optional via `PathResolver` protocol |
| Hard import of `sutil.bids.paths.BidsPaths` | Bundled or adapter-provided |
| `_BidsPathsReprProxy` wrapping | Not needed — pipeio controls its own types |
| `extra_in_paths` auto-discovery | Preserved — scan `input_dir_*` config keys |
| `stage_aliases` in config | Preserved — alias resolution in `PipelineContext.stage()` |
| `_member_sets` / `bundles` | Documented as config convention, not hard-coded |
