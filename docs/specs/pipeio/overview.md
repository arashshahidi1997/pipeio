# pipeio: Overview & Architecture

## Scope

pipeio manages computational pipeline workflows in research repositories. It provides:

- A **registry** for discovering and querying the pipe/flow/mod hierarchy
- **Flow configuration** loading and validation (declarative data contracts)
- **Path resolution** through a pluggable adapter protocol (generic core + BIDS adapter)
- **Notebook lifecycle** automation (pair, sync, execute, publish)
- **Scaffolding** for creating new flows and mods from templates
- **Contracts** for declarative input/output validation

## Ecosystem Position

| Subsystem | Domain | pipeio Relationship |
|-----------|--------|---------------------|
| **indexio** | retrieval | pipeio can register pipeline docs/configs as indexio sources |
| **biblio** | literature | Independent — no direct coupling |
| **notio** | notes | Experiment logs may reference pipeline flows |
| **codio** | code intelligence | codio discovers reusable code; pipeio manages where it runs |

pipeio is the first projio subsystem that manages **execution** rather than **information**. The others organize knowledge artifacts; pipeio organizes computational workflows and their outputs.

## Package Structure

```
packages/pipeio/
└── src/pipeio/
    ├── __init__.py
    ├── cli.py               # argparse CLI entry point
    ├── config.py             # FlowConfig: per-flow config.yml schema
    ├── registry.py           # PipelineRegistry: pipe/flow/mod hierarchy
    ├── resolver.py           # PathResolver protocol, PipelineContext, Session
    ├── contracts.py          # Declarative I/O validation
    ├── mcp.py                # MCP tool functions
    ├── notebook/
    │   ├── config.py         # NotebookConfig: notebook.yml schema
    │   ├── pair.py           # Jupytext pairing
    │   ├── sync.py           # Format sync (newer wins)
    │   ├── execute.py        # nbconvert execution
    │   └── publish.py        # Copy to docs directory
    ├── scaffold/
    │   ├── flow.py           # Flow scaffolding
    │   └── templates/        # Jinja2/YAML templates
    └── adapters/
        └── bids.py           # snakebids PathResolver adapter
```

## Dependency Tiers

```
pipeio              → pyyaml, pydantic (core)
pipeio[notebook]    → + jupytext, nbconvert
pipeio[bids]        → + snakebids
```

## Three-Level Hierarchy

The organizing principle is **pipe / flow / mod**:

```
pipe (e.g. preprocess, sharpwaveripple, spectrogram)
 └── flow (e.g. ieeg, ecephys, burst)
      └── mod (e.g. badchannel, linenoise, noise_tfspace)
```

- **pipe**: A scientific domain or processing stage
- **flow**: A concrete workflow — owns a `Snakefile`, `config.yml`, output directory, and notebooks
- **mod**: A logical module within a flow — a group of related rules identified by rule name prefix

This hierarchy is formalized in a registry YAML and can be discovered by scanning the filesystem.

## Configuration Layers

1. **Project config** (`.projio/config.yml` → `pipeio:` section) — project-level settings
2. **Pipeline registry** (`.pipeio/registry.yml`) — pipe/flow/mod hierarchy mapping
3. **Flow config** (`config.yml` per flow) — inputs, outputs, output registry (data contract)
4. **Notebook config** (`notebook.yml` per flow) — notebook lifecycle settings

## Integration with projio

### MCP Tools

pipeio exposes tools through projio's MCP server:

| Tool | Purpose |
|------|---------|
| `pipeio_flow_list` | List flows, optionally filtered by pipe |
| `pipeio_flow_status` | Status of a specific flow (config, outputs, notebooks) |
| `pipeio_nb_status` | Notebook sync and publication status |
| `pipeio_registry_validate` | Validate registry consistency |

### .projio/config.yml Section

```yaml
pipeio:
  enabled: true
  registry_path: .pipeio/registry.yml
  pipelines_dir: code/pipelines
  notebooks_dir: notebooks
```
