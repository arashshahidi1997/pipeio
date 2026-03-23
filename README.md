# pipeio

Pipeline registry, notebook lifecycle, and flow management for research repositories.

Part of the [projio](https://github.com/arashshahidi1997/projio) ecosystem.

## What pipeio does

pipeio manages computational pipeline workflows organized in a **pipe / flow / mod** hierarchy:

- **pipe** — a scientific domain (preprocessing, ripple detection, spectral analysis)
- **flow** — a concrete workflow with its own Snakefile, config, and output directory
- **mod** — a logical module within a flow (a group of related rules)

### Core features

- **Registry** — scan, load, validate, and query the pipe/flow/mod hierarchy
- **Flow config** — declarative output registry (data contracts) in `config.yml`
- **Path resolution** — generic resolver protocol with pluggable adapters (SimpleResolver built-in, BIDS optional)
- **Notebook lifecycle** — pair, sync, execute, and publish Jupytext notebooks *(planned)*
- **Scaffolding** — create new flows from templates
- **Contracts** — declarative input/output validation *(planned)*

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
