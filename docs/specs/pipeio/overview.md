# pipeio: Overview & Architecture

## Scope

pipeio is an **agent-facing authoring and discovery layer** for computational pipelines. It makes pipeline knowledge queryable and actionable for AI agents. It does **not** compete with execution engines (Snakemake), provenance systems (DataLad), app lifecycle managers (snakebids), or path resolvers (snakebids `bids()`).

### What pipeio provides

- A **registry** for discovering and querying the flow/mod hierarchy
- **AI-safe authoring** — `rule_insert`, `config_patch`, `mod_create` with validation
- **Contract semantics** — declarative I/O validation and cross-flow wiring
- **Flow configuration** loading and validation
- **Notebook lifecycle** automation (pair, sync, execute, publish)
- **Scaffolding** for creating new flows and mods from templates
- **Documentation** generation and collection

### Delegation model

| Concern | Delegated to | pipeio's role |
|---------|-------------|---------------|
| **Execution** | snakebids `run.py` → Snakemake | Registry/discovery, contract info for `--input`/`--output` |
| **Provenance** | DataLad run records | Contract semantics inform run metadata |
| **Path resolution** | snakebids `bids()` + `generate_inputs()` | Config authoring, not path computation |
| **App lifecycle** | snakebids deployment modes | Flow scaffolding, not deployment |

## Ecosystem Position

| Subsystem | Domain | pipeio Relationship |
|-----------|--------|---------------------|
| **indexio** | retrieval | pipeio can register pipeline docs/configs as indexio sources |
| **biblio** | literature | Independent — no direct coupling |
| **notio** | notes | Experiment logs may reference pipeline flows |
| **codio** | code intelligence | codio discovers reusable code; pipeio manages where it runs |

Like the other projio subsystems, pipeio organizes **knowledge** — pipeline metadata, contracts, and structure — rather than managing execution directly.

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

## Flow / Mod Hierarchy

The primary unit is the **flow** — a self-contained snakebids app producing one derivative directory:

```
flow (e.g. ieeg, ecephys, burst)
 └── mod (e.g. badchannel, linenoise, noise_tfspace)
```

- **flow**: A concrete workflow — owns a `Snakefile`, `config.yml`, output directory, and notebooks. Each flow produces one derivative.
- **pipe**: A category tag grouping related flows (e.g. `preprocess`, `spectral`). Not a hierarchical container — the pipe/flow nesting is being flattened.
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
