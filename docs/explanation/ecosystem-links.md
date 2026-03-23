# Ecosystem Links

Pipeio is part of the [projio](https://github.com/arashshahidi1997/projio) ecosystem — a suite of tools that turn a research repository into a queryable knowledge environment.

## Sibling packages

| Package | Domain | What it manages |
|---------|--------|-----------------|
| **projio** | Orchestration | Project scaffolding, MCP server, site building |
| **biblio** | Literature | Bibliography, citekey resolution, paper context |
| **indexio** | Retrieval | Corpus indexing, chunking, embedding, semantic search |
| **codio** | Code intelligence | Library registry, code reuse discovery |
| **notio** | Notes | Experiment logs, design decisions, idea capture |
| **pipeio** | Pipelines | Pipeline registry, notebook lifecycle, flow management |

## How pipeio connects

- **projio** registers pipeio's MCP tools (`pipeio_flow_list`, `pipeio_flow_status`, etc.) so AI agents can query pipeline state
- **indexio** can index pipeio's notebook outputs and pipeline documentation for semantic search
- **codio** may reference pipeline code as internal libraries in its catalog
- **notio** captures design decisions and experiment logs that reference specific pipeline flows

## Shared patterns

All ecosystem packages follow:

- **src-layout** with setuptools
- **argparse CLI** with subcommands
- **YAML configs** with Pydantic validation
- **MCP tool functions** called by projio's server
- **Editable install** for development: `pip install -e ".[dev]"`
