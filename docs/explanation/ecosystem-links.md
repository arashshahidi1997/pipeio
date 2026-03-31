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
| **pipeio** | Pipelines | Pipeline authoring, discovery, contracts, notebooks |

## Delegation model

pipeio is an agent-facing authoring and discovery layer. It does not own execution or provenance — those are delegated:

| Concern | Delegated to | pipeio's role |
|---------|-------------|---------------|
| **Execution** | snakebids `run.py` → Snakemake | Registry/discovery, contract data |
| **Provenance** | DataLad run records | Contract semantics inform `--input`/`--output` |
| **Path resolution** | snakebids `bids()` + `generate_inputs()` | Config authoring |
| **App lifecycle** | snakebids deployment modes | Flow scaffolding |

## How pipeio connects

- **projio** registers pipeio's MCP tools so AI agents can query and author pipeline structure
- **indexio** can index pipeio's notebook outputs and pipeline documentation for semantic search
- **codio** may reference pipeline code as internal libraries in its catalog
- **notio** captures design decisions and experiment logs that reference specific pipeline flows
- **DataLad** provides provenance for pipeline runs; pipeio's contract data informs `datalad run` input/output declarations

## Shared patterns

All ecosystem packages follow:

- **src-layout** with setuptools
- **argparse CLI** with subcommands
- **YAML configs** with Pydantic validation
- **MCP tool functions** called by projio's server
- **Editable install** for development: `pip install -e ".[dev]"`
