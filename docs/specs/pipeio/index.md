# pipeio Specifications

Design specifications for **pipeio** — an agent-facing authoring and discovery layer for computational pipelines in research repositories.

pipeio makes pipeline knowledge (registry, configs, rules, contracts, notebooks) queryable and actionable for AI agents. It delegates execution to Snakemake, provenance to DataLad, path resolution to snakebids, and app lifecycle to snakebids deployment modes.

## Spec Documents

| Spec | Domain | Status |
|------|--------|--------|
| [Ontology](ontology.md) | Flow/mod/rule hierarchy, directory layout, naming conventions, lifecycle states | **Canonical** |
| [Code Tiers](code-tiers.md) | Libraries, utils, and flow scripts — scaffolding, promotion, and audit across tiers | **Spec** |
| [Registry](registry.md) | Flow/mod hierarchy, YAML schema, scan & validation | Implemented |
| [Flow Config](flow-config.md) | Per-flow `config.yml` schema, output registry (data contracts) | Implemented |
| [Path Resolution](path-resolution.md) | `PathResolver` protocol, `PipelineContext`, `Session`, `Stage` | Implemented |
| [Notebook Lifecycle](notebook.md) | Workspaces (explore/demo), pair, sync, execute, publish, promote | Implemented |
| [Scaffolding](scaffolding.md) | Flow and mod creation, kind-aware notebooks, tier-aware scripts | Implemented |
| [Pipeline Docs](pipeline-docs.md) | Flow `docs/index.md` template, mod facets, flow `CHANGELOG.md`, collection pipeline | **Canonical** |
| [Contracts](contracts.md) | Declarative input/output validation, cross-flow manifest chains | Implemented |
| [CLI](cli.md) | Command-line interface design | Implemented |
| [MCP Tools](mcp-tools.md) | Agent-facing tools via projio MCP server | Implemented (50+ tools) |

## Design Principles

1. **Agent-facing authoring layer** — pipeio makes pipeline knowledge queryable and provides safe authoring operations; it does not own execution, provenance, or path resolution
2. **One flow = one derivative** — each flow is a self-contained snakebids app producing one derivative directory; flow names are globally unique
3. **Delegation over duplication** — execution → Snakemake, provenance → DataLad, paths → snakebids `bids()`, app lifecycle → snakebids
4. **Tier-aware scaffolding** — scripts and notebooks import from the right code tier (core library via codio, project utils via projio config)
5. **Graceful degradation** — pipeio works without optional extras (`[bids]`, `[notebook]`) and without codio/biblio/notio
6. **Search before creation** — registry queries help discover existing flows before scaffolding new ones
7. **Notebook as first-class artifact** — explore/demo workspaces, kind-aware templates, full lifecycle management
