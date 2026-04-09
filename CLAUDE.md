# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
make test              # run all tests (pytest, uses PYTHONPATH=src)
PYTHONPATH=src python -m pytest tests/ -q                        # all tests
PYTHONPATH=src python -m pytest tests/test_mcp.py -q             # single test file
PYTHONPATH=src python -m pytest tests/test_mcp.py::test_import -q  # single test
make build             # build distribution
make clean             # remove build artifacts
```

Python >= 3.11 required. Package source lives in `src/pipeio/` (src-layout via setuptools).

## What pipeio Is

pipeio is an **agent-facing authoring and discovery layer** for computational pipelines in research repositories. It makes pipeline knowledge — registry, configs, rules, contracts, notebooks — queryable and actionable for AI agents via MCP tools.

**North star:** pipeio does not compete with execution engines, provenance systems, app lifecycle managers, or path resolvers. It sits above them, providing structured access to pipeline metadata and safe authoring operations.

### Delegation Model

| Concern | Delegated to | pipeio's role |
|---------|-------------|---------------|
| **Execution** | snakebids `run.py` → Snakemake | Registry/discovery, not launching |
| **Provenance** | DataLad run records | Contract semantics inform `--input`/`--output` |
| **Path resolution** | snakebids `bids()` + `generate_inputs()` | Config authoring, not path computation |
| **App lifecycle** | snakebids deployment modes | Flow scaffolding, not deployment |

### What pipeio Owns

- **Registry & discovery** — scan, query, validate the flow/mod hierarchy
- **AI-safe authoring** — `rule_insert`, `config_patch`, `mod_create` with validation
- **Contract semantics** — I/O validation, cross-flow manifest wiring
- **Notebook lifecycle** — pair, sync, execute, publish (explore/demo workspaces)
- **Documentation** — collect (with publish.yml), nav generation, modkey bibliography

## Ontology

The canonical spec is `docs/specs/pipeio/ontology.md` in the parent projio repo. Key concepts:

### Flow

A **flow** is a self-contained snakebids app producing one derivative directory. Flow names are globally unique. Directory: `code/pipelines/{flow}/`.

### Mod

A **mod** is a logical group of Snakemake rules within a flow, identified by rule name prefix. Each mod has:
- Rules (in Snakefile or `rules/{mod}.smk`)
- Scripts (`scripts/{script}.py`) — may be shared across rules
- Documentation in three facets: `docs/{mod}/theory.md`, `spec.md`, `delta.md`
- Notebooks (explore or demo workspace)

### Mod Documentation Facets

Each mod has up to three doc facets in `{flow}/docs/{mod}/`:

| Facet | File | Purpose |
|-------|------|---------|
| **Theory** | `theory.md` | Scientific rationale, method justification, pandoc citations |
| **Spec** | `spec.md` | Technical specification: I/O contracts, parameters |
| **Delta** | `delta.md` | Temporary: current state, known issues, refactor plans |

### Notebook Workspaces

Notebooks live in two parallel workspaces within a flow:

- **`notebooks/explore/`** — prototypes, investigations. Never published. Findings feed theory.md.
- **`notebooks/demo/`** — showcases mod outputs. Published to site as HTML.

Both use `.src/` + `.myst/` + `.ipynb` layout. Kind routing:
- `investigate`, `explore` → `explore/` workspace
- `demo`, `validate` → `demo/` workspace

### Derivative Manifest

Each flow's derivative directory contains `manifest.yml` — a copy of the flow's `registry:` config section. Downstream flows reference it via `input_manifest` in their config.

```yaml
# Cross-flow wiring in downstream config.yml
input_dir: "derivatives/preprocess_ieeg"
input_manifest: "derivatives/preprocess_ieeg/manifest.yml"
```

### publish.yml

Per-flow config controlling which artifacts `docs_collect` publishes to the site:

```yaml
dag: true          # publish dag.svg
report: true       # publish report.html
scripts: true      # generate script index with git links
```

## Flow Directory Structure

```
code/pipelines/{flow}/
├── Snakefile
├── config.yml                     # input_dir, output_dir, input_manifest, output_manifest, registry
├── publish.yml                    # flow-level publish config
├── rules/                         # optional per-mod rule files
├── scripts/                       # rule scripts (may be shared)
├── docs/                          # flow-local docs
│   ├── index.md
│   └── {mod}/                     # per-mod faceted docs
│       ├── theory.md
│       ├── spec.md
│       └── delta.md               # optional, temporary
└── notebooks/
    ├── notebook.yml
    ├── explore/                   # exploratory notebooks
    │   ├── .src/
    │   ├── .myst/
    │   └── *.ipynb
    └── demo/                      # demo notebooks (published)
        ├── .src/
        ├── .myst/
        └── *.ipynb
```

## Registry Schema

```yaml
# .projio/pipeio/registry.yml
flows:
  preprocess_ieeg:
    name: preprocess_ieeg
    code_path: code/pipelines/preprocess_ieeg
    config_path: code/pipelines/preprocess_ieeg/config.yml
    mods:
      filter:
        name: filter
        rules: [filter_bandpass, filter_notch]
```

## CLI Surface

```
pipeio init                              — scaffold .pipeio/ in the current project
pipeio flow list [--prefix PREFIX]       — list all flows
pipeio flow new <flow>                   — scaffold a new flow
pipeio flow ids                          — print flow names (for shell completion)
pipeio flow path <flow>                  — print absolute code_path
pipeio flow config <flow>                — print absolute config_path
pipeio flow deriv <flow>                 — print absolute derivative directory
pipeio flow smk <flow> [smk_args]        — run snakemake in flow context
pipeio flow status <flow>                — show flow status
pipeio flow targets <flow> [-g/-m/-e/-x] — resolve output paths
pipeio flow run <flow> [-c/-n/-f]        — launch snakemake in screen session
pipeio flow mods <flow>                  — list mods and rules
pipeio nb pair|sync|exec|publish         — notebook lifecycle
pipeio nb status                         — notebook sync/publish status
pipeio registry scan                     — discover flows from filesystem
pipeio registry validate                 — validate registry consistency
pipeio contracts validate                — check pipeline I/O contracts
pipeio docs collect                      — collect flow docs → docs/pipelines/
pipeio docs nav                          — generate MkDocs nav fragment
```

### MCP Tool Surface

Tools exposed via projio's MCP server (44 tools across 10 categories):

**Flow & registry (4):** `pipeio_flow_list`, `pipeio_flow_status`, `pipeio_registry_scan`, `pipeio_registry_validate`

**Notebook lifecycle (15):** `pipeio_nb_status`, `pipeio_nb_create`, `pipeio_nb_update`, `pipeio_nb_move`, `pipeio_nb_sync`, `pipeio_nb_sync_flow`, `pipeio_nb_diff`, `pipeio_nb_scan`, `pipeio_nb_read`, `pipeio_nb_audit`, `pipeio_nb_lab`, `pipeio_nb_publish`, `pipeio_nb_analyze`, `pipeio_nb_exec`, `pipeio_nb_pipeline`

**Mod management (4):** `pipeio_mod_list`, `pipeio_mod_resolve`, `pipeio_mod_context`, `pipeio_mod_create`

**Rule authoring (4):** `pipeio_rule_list`, `pipeio_rule_stub`, `pipeio_rule_insert`, `pipeio_rule_update`

**Config authoring (3):** `pipeio_config_read`, `pipeio_config_patch`, `pipeio_config_init`

**Path resolution (1):** `pipeio_target_paths`

**Contracts & audit (5):** `pipeio_contracts_validate`, `pipeio_cross_flow`, `pipeio_completion`, `pipeio_mod_audit`, `pipeio_mod_doc_refresh`

**Scaffolding (2):** `pipeio_script_create`, `pipeio_nb_promote`

**DAG & reporting (2):** `pipeio_dag_export`, `pipeio_report`

**Logging (1):** `pipeio_log_parse`

**Documentation (4):** `pipeio_docs_collect`, `pipeio_docs_nav`, `pipeio_mkdocs_nav_patch`, `pipeio_modkey_bib`

**Execution (4):** `pipeio_run`, `pipeio_run_status`, `pipeio_run_dashboard`, `pipeio_run_kill`

## Source Layout

```
src/pipeio/
├── __init__.py          # public API exports
├── cli.py               # argparse CLI (pipeio command)
├── config.py            # FlowConfig (Pydantic model for config.yml)
├── registry.py          # PipelineRegistry (flow/mod hierarchy)
├── resolver.py          # PathResolver protocol, PipelineContext, Session
├── contracts.py         # Declarative I/O validation framework
├── docs.py              # docs_collect, docs_nav, PublishConfig
├── mcp.py               # MCP tool functions (called by projio MCP server)
├── notebook/
│   ├── __init__.py
│   ├── config.py        # NotebookConfig (notebook.yml model)
│   ├── lifecycle.py     # pair, sync, status, lab, publish
│   └── analyze.py       # Static notebook analysis
├── scaffold/
│   └── __init__.py      # Flow/mod scaffolding
├── adapters/
│   └── bids.py          # snakebids PathResolver adapter (optional)
└── templates/           # Jinja2/YAML templates
```

## Key Conventions (No Backward Compat)

- **No `pipe` parameter anywhere** — flows are addressed directly by name
- **`input_manifest` / `output_manifest`** — not `input_registry` / `output_registry`
- **Mod docs are faceted** — `docs/{mod}/theory.md` + `spec.md`, not single `mod-{mod}.md`
- **Notebooks use workspace dirs** — `explore/.src/` and `demo/.src/`, not flat `notebooks/.src/`
- **Modkey format** — `{flow}_mod-{mod}`, not `pipe-X_flow-Y_mod-Z`
- **Published docs path** — `docs/pipelines/{flow}/`, not `docs/pipelines/{pipe}/{flow}/`

## Project Context

This is part of the projio ecosystem. The ontology spec lives at `docs/specs/pipeio/ontology.md` in the parent projio repo. The reference implementation was extracted from the pixecog project's pipeline infrastructure.
