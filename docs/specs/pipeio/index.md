# pipeio Specifications

Design specifications for **pipeio** — an agent-facing authoring and discovery layer for computational pipelines in research repositories.

pipeio makes pipeline knowledge (registry, configs, rules, contracts, notebooks) queryable and actionable for AI agents. It delegates execution to Snakemake, provenance to DataLad, path resolution to snakebids, and app lifecycle to snakebids deployment modes.

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

1. **Agent-facing authoring layer** — pipeio makes pipeline knowledge queryable and provides safe authoring operations; it does not own execution, provenance, or path resolution
2. **One flow = one derivative** — each flow is a self-contained snakebids app producing one derivative directory; `pipe` is a category tag
3. **Delegation over duplication** — execution → Snakemake, provenance → DataLad, paths → snakebids `bids()`, app lifecycle → snakebids
4. **Declarative over imperative** — registries and configs are YAML; validation is schema-driven
5. **Graceful degradation** — pipeio works without optional extras (`[bids]`, `[notebook]`)
6. **Search before creation** — registry queries help discover existing flows before scaffolding new ones
7. **Notebook as first-class artifact** — the lifecycle (pair/sync/exec/publish) is managed, not ad-hoc
