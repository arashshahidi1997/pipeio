# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
make test              # run all tests (pytest, uses PYTHONPATH=src)
PYTHONPATH=src python -m pytest tests/ -q                        # all tests
PYTHONPATH=src python -m pytest tests/test_pipeio.py -q          # single test file
PYTHONPATH=src python -m pytest tests/test_pipeio.py::test_import -q  # single test
make build             # build distribution
make clean             # remove build artifacts
```

Python >= 3.11 required. Package source lives in `src/pipeio/` (src-layout via setuptools).

## What pipeio Is

pipeio is an **agent-facing authoring and discovery layer** for computational pipelines in research repositories. It makes pipeline knowledge — registry, configs, rules, contracts, notebooks — queryable and actionable for AI agents via MCP tools.

**North star:** pipeio does not compete with execution engines, provenance systems, app lifecycle managers, or path resolvers. It sits above them, providing structured access to pipeline metadata and safe authoring operations.

### Delegation Model

pipeio delegates core pipeline concerns to specialized tools:

| Concern | Delegated to | pipeio's role |
|---------|-------------|---------------|
| **Execution** | snakebids `run.py` → Snakemake | Registry/discovery, not launching |
| **Provenance** | DataLad run records | Contract semantics inform `--input`/`--output` |
| **Path resolution** | snakebids `bids()` + `generate_inputs()` | Config authoring, not path computation |
| **App lifecycle** | snakebids deployment modes | Flow scaffolding, not deployment |

### What pipeio Owns

- **Registry & discovery** — scan, query, validate the flow/mod hierarchy
- **AI-safe authoring** — `rule_insert`, `config_patch`, `mod_create` with validation
- **Contract semantics** — I/O validation, cross-flow wiring
- **Notebook lifecycle** — pair, sync, execute, publish
- **Documentation** — collect, nav generation, modkey bibliography

### One Flow = One Derivative

Each flow is a self-contained snakebids app producing one derivative directory. The `pipe` field is a **category tag** (e.g. `preprocess`, `spectral`) — not a hierarchical container. Flows are the primary unit of organization.

### Ecosystem Siblings

| Tool | Purpose | Scaffolds | CLI entry |
|------|---------|-----------|-----------|
| **projio** | Project orchestration | `.projio/` | `projio init` |
| **biblio** | Bibliography/papers | `bib/` | `biblio init` |
| **indexio** | Semantic indexing/RAG | `infra/indexio/` | `indexio init-config` |
| **codio** | Code reuse discovery | `.codio/` | `codio init` |
| **notio** | Structured notes | `.notio/` | `notio init` |
| **pipeio** | Pipeline authoring & discovery | `.pipeio/` | `pipeio init` |

All tools share: src-layout, argparse CLI, YAML configs, Pydantic models, projio MCP integration.

## Architecture

### Flow / Mod Hierarchy

- **flow**: A self-contained snakebids app — owns a Snakefile, config.yml, output directory, and notebooks. Each flow produces one derivative directory.
- **pipe**: A category tag grouping related flows (e.g. `preprocess`, `spectral`). Not a hierarchical container.
- **mod**: A logical module within a flow — a group of related rules (identified by rule name prefix)

### Core Modules

- `config.py` — `FlowConfig`: load/validate per-flow `config.yml` with output registry schema
- `registry.py` — `PipelineRegistry`: scan, load, validate, query the pipe/flow/mod hierarchy
- `resolver.py` — `PathResolver` protocol + `PipelineContext` + `Session` for path resolution
- `contracts.py` — Declarative input/output validation framework
- `notebook/config.py` — `NotebookConfig`: load/validate `notebook.yml`
- `adapters/bids.py` — snakebids adapter for `PathResolver` (requires `pipeio[bids]`)
- `mcp.py` — MCP tool functions (called by projio's MCP server)
- `cli.py` — argparse CLI entry point

### PathResolver Protocol

The key abstraction is `PathResolver` — a protocol that adapters implement to translate generic (group, member, entities) tuples into concrete filesystem paths:

```python
class PathResolver(Protocol):
    def resolve(self, group: str, member: str, **entities: str) -> Path: ...
    def expand(self, group: str, member: str, **filters: str) -> list[Path]: ...
```

`PipelineContext` and `Session` use this protocol, making the core workflow-engine-agnostic. The BIDS/snakebids adapter (`adapters/bids.py`) is one concrete implementation, gated behind `pipeio[bids]`.

### CLI Surface

```
pipeio init                       — scaffold .pipeio/ in the current project
pipeio flow list                  — list all flows
pipeio flow new <pipe> <flow>     — scaffold a new flow
pipeio nb pair|sync|exec|publish  — notebook lifecycle
pipeio nb status                  — notebook sync/publish status
pipeio registry scan              — discover flows from filesystem
pipeio registry validate          — validate registry consistency
pipeio contracts validate         — check pipeline I/O contracts
```

### MCP Tool Surface

Tools exposed via projio's MCP server (35 tools across 7 categories):

**Flow & registry:** `pipeio_flow_list`, `pipeio_flow_status`, `pipeio_registry_scan`, `pipeio_registry_validate`

**Notebook lifecycle:** `pipeio_nb_status`, `pipeio_nb_create`, `pipeio_nb_sync`, `pipeio_nb_diff`, `pipeio_nb_publish`, `pipeio_nb_analyze`, `pipeio_nb_exec`, `pipeio_nb_pipeline`

**Mod management:** `pipeio_mod_list`, `pipeio_mod_resolve`, `pipeio_mod_create` (with I/O wiring + PipelineContext support)

**Rule authoring:** `pipeio_rule_list`, `pipeio_rule_stub`, `pipeio_rule_insert`, `pipeio_rule_update`

**Config authoring:** `pipeio_config_read`, `pipeio_config_patch`, `pipeio_config_init`

**Path resolution:** `pipeio_target_paths`

**Contracts & tracking:** `pipeio_contracts_validate`, `pipeio_cross_flow`, `pipeio_completion`

**Adapters** *(thin wrappers, may migrate to datalad run)*: `pipeio_dag_export`, `pipeio_log_parse`, `pipeio_config_init`

**Deprecated** *(to be replaced by datalad run)*: `pipeio_run`, `pipeio_run_status`, `pipeio_run_dashboard`, `pipeio_run_kill`

**Documentation:** `pipeio_docs_collect`, `pipeio_docs_nav`, `pipeio_mkdocs_nav_patch`, `pipeio_modkey_bib`

## Source Layout

```
src/pipeio/
├── __init__.py          # public API exports
├── cli.py               # argparse CLI (pipeio command)
├── config.py            # FlowConfig (Pydantic model for config.yml)
├── registry.py          # PipelineRegistry (pipe/flow/mod hierarchy)
├── resolver.py          # PathResolver protocol, PipelineContext, Session
├── contracts.py         # Declarative I/O validation framework
├── mcp.py               # MCP tool functions (called by projio MCP server)
├── notebook/
│   ├── __init__.py
│   └── config.py        # NotebookConfig (Pydantic model for notebook.yml)
├── scaffold/
│   └── __init__.py      # Flow/mod scaffolding from templates
├── adapters/
│   ├── __init__.py
│   └── bids.py          # snakebids PathResolver adapter (optional)
└── templates/           # Jinja2/YAML templates for scaffold
```

## Optional Dependencies

- `pipeio[bids]` — snakebids adapter for BIDS-compliant path resolution
- `pipeio[notebook]` — jupytext + nbconvert for notebook lifecycle

## Project Context

This is part of the projio ecosystem. Specs live in the parent projio repo at `docs/specs/pipeio/`. The reference implementation was extracted from the pixecog project's pipeline infrastructure.
