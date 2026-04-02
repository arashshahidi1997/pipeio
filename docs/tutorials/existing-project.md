# Adopting pipeio in an existing project

This tutorial shows how to add pipeio to a project that already has Snakemake pipelines.

## Prerequisites

- Python 3.11+
- A project with Snakemake pipelines (Snakefiles and/or `.smk` files)
- projio installed and configured (optional, for MCP integration)

## 1. Install and initialize

```bash
pip install pipeio
cd ~/projects/my-study
pipeio init
```

If `.projio/` exists (projio-managed project), pipeio scaffolds under `.projio/pipeio/`. Otherwise it creates `.pipeio/`.

## 2. Auto-discover flows

Pipeio scans `code/pipelines/` (or `pipelines/`) for directories with a Snakefile or `config.yml`.

```bash
pipeio registry scan
```

Example output:

```
Scanned code/pipelines
  brainstate         config=yes  mods=3
  preprocess_ieeg    config=yes  mods=5
  preprocess_ecephys config=yes  mods=2
  spectrogram_burst  config=yes  mods=4
Written: .projio/pipeio/registry.yml (4 flows)
```

### Mod auto-discovery

Within each flow, pipeio parses the Snakefile for `rule` blocks and groups them by name prefix. Rules `filter_raw`, `filter_notch` → mod `filter`.

## 3. Augment existing flows

Run `flow new` on existing flows to fill in any missing structure:

```bash
pipeio flow new preprocess_ieeg
```

This is **idempotent** — it adds missing directories (rules/, explore/, demo/, publish.yml) without touching existing files (Snakefile, config.yml).

## 4. Migrate config keys

If your configs use old-style keys, rename:

| Old | New |
|-----|-----|
| `input_registry` | `input_manifest` |
| `output_registry` | `output_manifest` |

Manifest files should be named `manifest.yml` (not `*_registry.yml`).

## 5. Migrate mod docs

If you have `docs/mod-filter.md` files, migrate to faceted format:

```
docs/mod-filter.md  →  docs/filter/theory.md + docs/filter/spec.md
```

Move the old content into `theory.md`. Create `spec.md` stubs, then auto-generate:

```python
pipeio_mod_doc_refresh(flow="preprocess_ieeg", mod="filter", facet="spec", apply=True)
```

## 6. Migrate notebooks

If notebooks are in flat `notebooks/.src/`, route to workspaces:

- `investigate_*` and `explore_*` → `notebooks/explore/.src/`
- `demo_*` and `validate_*` → `notebooks/demo/.src/`

Update paths in `notebook.yml` accordingly.

## 7. Sync code libraries

If your project has compute libraries in `code/lib/` or project utils in `code/utils/`, register them:

```bash
projio sync                   # auto-discover code/lib/*/ → codio (role=core)
                              # auto-detect code/utils/ → code.project_utils config
```

This enables tier-aware scaffolding: `nb_create`, `script_create`, and `mod_create` will auto-import your compute library and project utils in generated templates.

## 8. Validate

```bash
pipeio registry validate      # registry consistency
pipeio contracts validate     # I/O contracts
```

Via MCP:

```python
pipeio_mod_audit(flow="preprocess_ieeg")   # per-mod health check
pipeio_nb_audit()                           # notebook lifecycle issues
```

## 9. Use via MCP (agent integration)

All pipeio tools are available to AI agents via the projio MCP server.
Call `skill_read("pipeio-guide")` for the full tool reference and agentic workflows.

Key tools for existing projects:

| Intent | Tool |
|--------|------|
| Discover flows | `pipeio_flow_list()`, `pipeio_registry_scan()` |
| Understand a mod | `pipeio_mod_context(flow, mod)` — rules, scripts, docs, config in one call |
| Audit health | `pipeio_mod_audit(flow)` — contract drift, missing docs/scripts |
| Refresh docs | `pipeio_mod_doc_refresh(flow, mod, "spec", apply=True)` |
| Add a new mod | `pipeio_mod_create(flow, mod, inputs, outputs)` |

## Project structure after adoption

```
my-study/
├── .projio/pipeio/
│   └── registry.yml
├── code/pipelines/
│   ├── preprocess_ieeg/
│   │   ├── Snakefile
│   │   ├── config.yml           # input_manifest, output_manifest, registry
│   │   ├── publish.yml          # dag, report, scripts
│   │   ├── rules/
│   │   ├── scripts/
│   │   ├── docs/
│   │   │   ├── index.md
│   │   │   └── filter/          # per-mod faceted docs
│   │   │       ├── theory.md
│   │   │       └── spec.md
│   │   └── notebooks/
│   │       ├── notebook.yml
│   │       ├── explore/.src/    # exploratory
│   │       └── demo/.src/       # published demos
│   └── brainstate/
│       └── ...
└── derivatives/
    ├── preprocess_ieeg/
    │   └── manifest.yml
    └── brainstate/
        └── manifest.yml
```
