# Adopting pipeio in an existing project

This tutorial shows how to add pipeio to a project that already has Snakemake pipelines. It covers installation, auto-discovery, MCP tool usage, and ongoing registry maintenance. The example uses a project with pipelines under `code/pipelines/`.

## Prerequisites

- Python 3.11+
- A project with Snakemake pipelines (Snakefiles and/or `.smk` files)
- projio installed and configured (optional, for MCP integration)

## 1. Install pipeio

```bash
pip install pipeio
```

Or from a local checkout (development):

```bash
pip install -e /path/to/pipeio
```

## 2. Initialize the workspace

```bash
cd ~/projects/my-study
pipeio init
```

If `.projio/` exists (projio-managed project), pipeio scaffolds under `.projio/pipeio/`. Otherwise it creates `.pipeio/`.

```
.projio/pipeio/         # or .pipeio/
  registry.yml          # pipeline registry (initially empty)
```

## 3. Auto-discover pipelines

Pipeio scans for pipelines in two locations (checked in order):

1. `code/pipelines/` — common in study-type projects
2. `pipelines/` — common in tool-type projects

Each top-level directory becomes a **pipe**. Subdirectories with a `Snakefile` or `config.yml` become **flows**. If a directory is itself both a pipe and a flow (has a Snakefile at root), it becomes a single-flow pipe.

```bash
pipeio registry scan
```

Example output for a neuroscience project:

```
Scanned code/pipelines
  pipe=brainstate     flow=brainstate     config=yes  mods=3
  pipe=preprocess     flow=ecephys        config=yes  mods=2
  pipe=preprocess     flow=ieeg           config=yes  mods=5
  pipe=spectrogram    flow=spectrogram    config=yes  mods=4
Written: .projio/pipeio/registry.yml (4 pipes, 4 flows)
```

### Mod auto-discovery

Within each flow, pipeio parses the Snakefile for `rule` blocks and groups them by name prefix. For example, rules named `filter_raw`, `filter_notch`, `filter_bandpass` are grouped into a mod called `filter`. It also cross-references `docs/mod-<name>.md` for documentation paths.

## 4. Inspect the registry

List discovered flows:

```bash
pipeio flow list
```

Check a specific flow's status (config, outputs, notebooks):

```bash
pipeio flow list --pipe preprocess
```

Validate registry consistency:

```bash
pipeio registry validate
```

This checks that code paths exist, configs parse correctly, and doc references are valid.

## 5. Use via MCP (agent integration)

If projio is installed and the MCP server is configured, all pipeio tools are available to AI agents. The MCP server exposes:

| Tool | Description |
|------|-------------|
| `pipeio_flow_list` | List flows, optionally filtered by pipe |
| `pipeio_flow_status` | Status of a specific flow (config, outputs, notebooks) |
| `pipeio_nb_status` | Notebook sync/publish status across flows |
| `pipeio_mod_list` | List mods within a flow |
| `pipeio_mod_resolve` | Resolve modkeys to metadata and doc paths |
| `pipeio_registry_scan` | Re-scan filesystem and rebuild registry |
| `pipeio_registry_validate` | Validate registry consistency |
| `pipeio_docs_collect` | Collect flow-local docs and notebooks into `docs/pipelines/` |
| `pipeio_docs_nav` | Generate MkDocs nav YAML fragment for pipeline docs |
| `pipeio_contracts_validate` | Validate I/O contracts (config, dirs, registry groups) |

An agent can trigger auto-discovery via `pipeio_registry_scan` without needing CLI access. This is useful when pipelines are added or restructured.

### Modkeys

Mods are addressable via **modkeys** — structured identifiers of the form `pipe-<pipe>_flow-<flow>_mod-<mod>`. For example:

```
pipe-preprocess_flow-ieeg_mod-filter
pipe-brainstate_flow-brainstate_mod-detect
```

Use `pipeio_mod_resolve` to look up metadata and documentation for a list of modkeys.

## 6. Keep the registry current

After adding or restructuring pipelines, re-scan:

```bash
pipeio registry scan
```

Or via MCP: call `pipeio_registry_scan`.

The registry is a generated artifact — treat it like a lockfile. Re-scan when the pipeline structure changes; don't hand-edit it.

## Project structure reference

A typical study project after pipeio adoption:

```
my-study/
  .projio/
    pipeio/
      registry.yml              # auto-generated
    config.yml                  # projio config
  code/
    pipelines/
      preprocess/
        ieeg/
          Snakefile
          config.yml
          docs/
            mod-filter.md
            mod-rereference.md
        ecephys/
          Snakefile
          config.yml
      brainstate/
        brainstate.smk
        config.yml
  workflow/
    Snakefile                   # top-level orchestrator (imports from code/pipelines/)
```

## What's next

- [Quickstart](quickstart.md) — creating flows from scratch
- [How-to: Registry](../how-to/registry.md) — scan, validate, customize
- [Explanation: Hierarchy](../explanation/hierarchy.md) — pipe / flow / mod design
- [Explanation: Ecosystem Links](../explanation/ecosystem-links.md) — how pipeio connects to projio, codio, notio
