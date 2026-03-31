# pipeio

Agent-facing authoring and discovery layer for computational pipelines in research repositories.

Part of the [projio](https://github.com/arashshahidi1997/projio) ecosystem.

## What pipeio does

pipeio makes pipeline knowledge queryable and actionable for AI agents. It does not compete with execution engines (Snakemake), provenance systems (DataLad), app lifecycle managers (snakebids), or path resolvers (snakebids `bids()`).

Each **flow** is a self-contained snakebids app producing one derivative directory. The `pipe` field is a category tag grouping related flows. **Mods** are logical modules (rule groups) within a flow.

### Core features

- **Registry & discovery** — scan, load, validate, and query the flow/mod hierarchy
- **AI-safe authoring** — `rule_insert`, `config_patch`, `mod_create` with validation
- **Contract semantics** — declarative I/O validation and cross-flow wiring
- **Flow config** — declarative output registry (data contracts) in `config.yml`
- **Notebook lifecycle** — pair, sync, execute, and publish Jupytext notebooks
- **Scaffolding** — create new flows and mods from templates
- **Documentation** — collect, nav generation, modkey bibliography

## Install

```bash
pip install pipeio                # core
pip install pipeio[notebook]      # + jupytext/nbconvert
pip install pipeio[bids]          # + snakebids adapter
```

## CLI

```
pipeio init                       # scaffold .pipeio/ in the current project
pipeio flow list [--pipe PIPE]    # list all flows
pipeio flow new <pipe> <flow>     # scaffold a new flow
pipeio registry scan              # discover flows from filesystem
pipeio registry validate          # validate registry consistency
```

## Python API

```python
from pipeio import FlowConfig, PipelineContext

cfg = FlowConfig.from_yaml(Path("code/pipelines/preprocess/ieeg/config.yml"))
ctx = PipelineContext.from_config(cfg, root=Path("."))

sess = ctx.session(subject="01", session="pre")
path = sess.get("badlabel", "npy")
paths = sess.bundle("badlabel")  # {'npy': Path(...), 'featuremap': Path(...)}
```

## Implementation status

| Module | Status | Description |
|--------|--------|-------------|
| Registry | **Done** | scan, load, validate, query, YAML round-trip |
| Flow config | **Done** | load, extra inputs, groups/products, validation |
| Path resolution | **Done** | SimpleResolver, PipelineContext, Session, Stage |
| CLI | **Done** | init, flow list/new, registry scan/validate |
| MCP tools | **Done** | flow_list, flow_status, nb_status, registry_validate |
| Contracts | Partial | Models defined, CLI not wired |
| Notebook lifecycle | Planned | Requires `pipeio[notebook]` |
| BIDS adapter | Stub | Requires `pipeio[bids]` |

## Development

```bash
pip install -e ".[dev]"
make test
```
