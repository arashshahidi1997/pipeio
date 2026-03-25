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

pipeio is a **standalone CLI tool and library** for pipeline registry, notebook lifecycle, and flow management in research repositories. It manages computational workflows organized in a **pipe / flow / mod** hierarchy and provides notebook lifecycle automation (pair/sync/exec/publish).

When you run `pipeio init` in a project, it scaffolds a `.pipeio/` directory with registry and config files. The CLI and MCP tools then operate on that project's registry.

### Ecosystem Siblings

pipeio follows the same architectural patterns as its sibling tools:

| Tool | Purpose | Scaffolds | CLI entry |
|------|---------|-----------|-----------|
| **projio** | Project orchestration | `.projio/` | `projio init` |
| **biblio** | Bibliography/papers | `bib/` | `biblio init` |
| **indexio** | Semantic indexing/RAG | `infra/indexio/` | `indexio init-config` |
| **codio** | Code reuse discovery | `.codio/` | `codio init` |
| **notio** | Structured notes | `.notio/` | `notio init` |
| **pipeio** | Pipeline management | `.pipeio/` | `pipeio init` |

All tools share: src-layout, argparse CLI, YAML configs, Pydantic models, projio MCP integration.

## Architecture

### Three-Level Hierarchy: pipe / flow / mod

- **pipe**: A scientific domain (preprocessing, ripple detection, spectral analysis)
- **flow**: A concrete workflow with its own Snakefile, config.yml, and output directory
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

Tools exposed via projio's MCP server: `pipeio_flow_list`, `pipeio_flow_status`, `pipeio_nb_status`, `pipeio_nb_create`, `pipeio_nb_sync`, `pipeio_nb_publish`, `pipeio_mod_list`, `pipeio_mod_resolve`, `pipeio_registry_scan`, `pipeio_registry_validate`, `pipeio_docs_collect`, `pipeio_docs_nav`, `pipeio_contracts_validate`.

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
