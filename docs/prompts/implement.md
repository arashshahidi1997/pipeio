# Task: Implement pipeio core modules

## Context

pipeio is a projio ecosystem subpackage at `packages/pipeio/`. The skeleton
(pyproject.toml, CLAUDE.md, stub modules, Pydantic models, CLI parser, MCP
integration) and detailed specs (`docs/specs/pipeio/`) are already in place.
All stubs currently raise NotImplementedError or print "not yet implemented".

The reference implementation lives in pixecog at:
- `code/utils/io/context.py` — PipelineContext (from_registry, from_config, path, have, expand, session, stage, iter_sessions, parse_entities, groups, products, pattern, bundle_members)
- `code/utils/io/registry.py` — PipelineRegistry, PipelineEntry, load_pipeline_registry, get_pipeline_entry, get_pipeline_config
- `code/utils/io/session.py` — Session (get, have, bundle)
- `code/utils/io/stage.py` — Stage (paths, have, resolve)
- `code/utils/cli/gen_pipe_flow_mod_registry.py` — registry scanner
- `code/pipelines/_template/flow/Makefile` — notebook lifecycle (350 lines bash)
- `code/pipelines/_template/flow/notebooks/notebook.yml` — notebook config schema
- `code/pipelines/sharpwaveripple/contracts.py` — validate_inputs/validate_outputs

Pixecog root: /storage2/arash/projects/pixecog

## Implementation order

Work through these phases. Each phase should result in passing tests.

### Phase 1: Registry & Config (core, no optional deps)

Flesh out `registry.py` and `config.py` so they can:

1. **PipelineRegistry** — `from_yaml()` already works. Add:
   - `get(pipe, flow)` → FlowEntry (with flow-resolution logic: single-flow auto-select, pipe=flow shortcut)
   - `scan(pipelines_dir, docs_dir=None)` → class method that discovers flows from filesystem (look for Snakefile/config.yml), returns a new PipelineRegistry
   - `validate()` → returns errors/warnings (slug check, config existence, entrypoint existence)
   - `to_yaml(path)` — serialize back to YAML

2. **FlowConfig** — `from_yaml()` works. Add:
   - `extra_inputs()` → discover `input_dir_<name>` + `input_registry_<name>` pairs from `extra`
   - `validate()` → check required fields, member completeness
   - `groups()` / `products(group)` — convenience accessors for registry section

Read the pixecog `registry.py` and `gen_pipe_flow_mod_registry.py` for the
flow-resolution logic and scan patterns. The pipeio version should be
project-agnostic (no REPO_ROOT, no snakebids import).

### Phase 2: Path Resolution (resolver.py)

Implement the full PipelineContext and Session without requiring snakebids.
Add a `SimpleResolver` adapter (non-BIDS) that constructs paths from the
config registry using simple string template expansion:

```
{output_dir}/{root}/{subject}/{member.suffix}{member.extension}
```

This gives pipeio a working resolver for projects that don't use BIDS.
Keep the BidsResolver stub as-is (requires pipeio[bids]).

Add to PipelineContext:
- `from_registry(pipe, flow, root, registry=None)` — factory method
- `from_config(flow_config, root)` — factory from loaded FlowConfig
- `groups()`, `products(group)`, `pattern(group, member)`
- `path()`, `have()`, `expand()`
- `stage(name)` → Stage handle (with alias resolution from config `stage_aliases`)

Add to Session:
- `bundle(group)` — return {member: Path} for all members in a group

Add Stage class:
- `paths(sess, members=None)`, `have(sess, members=None)`, `resolve(sess, prefer)`

Reference: pixecog `context.py`, `session.py`, `stage.py`.

### Phase 3: CLI wiring

Wire the argparse subcommands to actual implementations:
- `pipeio init` → scaffold `.pipeio/` with empty registry
- `pipeio flow list` → load registry, print flows
- `pipeio flow new` → copy template + variable substitution
- `pipeio registry scan` → PipelineRegistry.scan() → write YAML
- `pipeio registry validate` → load + validate, print results

### Phase 4: Notebook lifecycle

Implement `notebook/pair.py`, `notebook/sync.py`, `notebook/execute.py`,
`notebook/publish.py`. These wrap jupytext and nbconvert. Gate imports
behind `pipeio[notebook]` availability check.

Wire to CLI: `pipeio nb pair|sync|exec|publish|status`

Reference: pixecog flow Makefile targets — translate the bash into Python.

### Phase 5: MCP tools

Replace NotImplementedError in `mcp.py` with real implementations
calling the modules from phases 1-4.

## Guidelines

- Follow the existing patterns from codio/biblio (Pydantic models, argparse
  CLI, yaml configs, src-layout)
- Keep the core free of snakebids/BIDS imports — those belong in adapters/
- Write tests alongside each phase in `tests/`
- The specs at `docs/specs/pipeio/` are the source of truth for behavior
- Read the pixecog reference code before implementing each module —
  don't guess at behavior, study the working code
- Use `@dataclass(frozen=True, slots=True)` for PipelineContext, Session,
  Stage (matching pixecog's pattern)
