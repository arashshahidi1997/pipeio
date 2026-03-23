# pipeio Specifications

Design specifications for **pipeio** — pipeline registry, notebook lifecycle, and flow management for research repositories.

pipeio is the fifth ecosystem subsystem in projio, managing computational pipeline workflows organized in a **pipe / flow / mod** hierarchy.

## Spec Documents

| Spec | Domain | Status |
|------|--------|--------|
| [Overview & Architecture](overview.md) | Package scope, design principles, ecosystem fit | Draft |
| [Registry](registry.md) | Pipe/flow/mod hierarchy, YAML schema, scan & validation | **Implemented** |
| [Flow Config](flow-config.md) | Per-flow `config.yml` schema, output registry (data contracts) | **Implemented** |
| [Path Resolution](path-resolution.md) | `PathResolver` protocol, `PipelineContext`, `Session`, `Stage` | **Implemented** (SimpleResolver) |
| [Notebook Lifecycle](notebook.md) | Pair, sync, execute, publish — replacing Makefile shell scripts | Draft |
| [Scaffolding](scaffolding.md) | Flow and mod creation from templates | Partial (`flow new` works) |
| [Contracts](contracts.md) | Declarative input/output validation framework | Draft (models defined) |
| [CLI](cli.md) | Command-line interface design | **Implemented** (core commands) |
| [MCP Tools](mcp-tools.md) | Agent-facing tools via projio MCP server | **Implemented** |

## Implementation Status

| Module | Status | Notes |
|--------|--------|-------|
| `registry.py` | **Done** | `from_yaml`, `to_yaml`, `get`, `scan`, `validate`, `slug_ok` |
| `config.py` | **Done** | `from_yaml`, `extra_inputs`, `groups`, `products`, `validate_config` |
| `resolver.py` | **Done** | `PathResolver` protocol, `SimpleResolver`, `PipelineContext`, `Session`, `Stage` |
| `cli.py` | **Done** | `init`, `flow list`, `flow new`, `registry scan`, `registry validate` |
| `mcp.py` | **Done** | `mcp_flow_list`, `mcp_flow_status`, `mcp_nb_status`, `mcp_registry_validate` |
| `contracts.py` | Partial | Models defined (`Check`, `Contract`, `ContractResult`), no CLI wiring |
| `notebook/` | Stub | Config models only; lifecycle (pair/sync/exec/publish) not implemented |
| `adapters/bids.py` | Stub | `BidsResolver` shell; requires `pipeio[bids]` |
| `scaffold/` | Stub | `flow new` works via CLI; no template engine yet |

## Reference Implementation

These specs are derived from an audit of the pixecog project's pipeline infrastructure (`code/utils/io/`, `code/pipelines/`, `workflow/`). The audit document lives at `pixecog/prompts/plan/pipeio-audit-and-design.md`.

## Design Principles

1. **Workflow-engine-agnostic core** — the `PathResolver` protocol abstracts away Snakemake/snakebids; adapters implement it
2. **Declarative over imperative** — registries and configs are YAML; validation is schema-driven
3. **Graceful degradation** — pipeio works without optional extras (`[bids]`, `[notebook]`)
4. **Search before creation** — registry queries help discover existing flows before scaffolding new ones
5. **Notebook as first-class artifact** — the lifecycle (pair/sync/exec/publish) is managed, not ad-hoc
