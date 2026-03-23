# Quickstart

This tutorial walks through setting up pipeio in a project, scanning for pipeline flows, and resolving output paths. By the end you will have a working `.pipeio/` registry and understand the core workflow.

## Prerequisites

- Python 3.11+
- pipeio installed: `pip install pipeio`
- A project directory to work in

## 1. Initialize the workspace

Pick a project directory and scaffold the `.pipeio/` workspace:

```bash
cd ~/projects/my-project
pipeio init
```

This creates:

```
.pipeio/
  registry.yml          # empty pipeline registry
  templates/
    flow/               # default flow template
```

## 2. Create a flow

Scaffold a new pipeline flow:

```bash
pipeio flow new preprocess ieeg
```

This creates:

```
pipelines/preprocess/ieeg/
  config.yml            # flow configuration (inputs, outputs, registry)
  Snakefile             # workflow entry point
```

## 3. Define the output registry

Open `pipelines/preprocess/ieeg/config.yml` and define your pipeline's data contract:

```yaml
input_dir: "raw"
output_dir: "derivatives/preprocess"

registry:
  raw_zarr:
    bids:
      root: "raw_zarr"
    members:
      zarr: { suffix: "ieeg", extension: ".zarr" }

  badlabel:
    bids:
      root: "badlabel"
    members:
      npy: { suffix: "ieeg", extension: ".npy" }
      featuremap: { suffix: "ieeg", extension: ".featuremap.png" }
```

The `registry` section declares output groups and their members. Each group is a pipeline stage; each member is a named output product with a suffix and extension.

## 4. Scan the filesystem

Discover all flows and write the registry:

```bash
pipeio registry scan --pipelines-dir pipelines
```

```
Scanned pipelines
  pipe=preprocess  flow=ieeg  config=yes
Written: .pipeio/registry.yml (1 pipes, 1 flows)
```

## 5. List and validate flows

```bash
pipeio flow list
```

```
  preprocess/ieeg  code=pipelines/preprocess/ieeg  config=pipelines/preprocess/ieeg/config.yml
```

Check registry consistency:

```bash
pipeio registry validate
```

```
Registry is valid.
```

## 6. Use the Python API

Load the config and resolve paths programmatically:

```python
from pathlib import Path
from pipeio import FlowConfig, PipelineContext

# Load the flow config
cfg = FlowConfig.from_yaml(Path("pipelines/preprocess/ieeg/config.yml"))

# List groups and members
cfg.groups()                    # ['badlabel', 'raw_zarr']
cfg.products("badlabel")       # ['npy', 'featuremap']

# Create a context for path resolution
ctx = PipelineContext.from_config(cfg, root=Path("."))

# Create a session with bound entities
sess = ctx.session(subject="01", session="pre")

# Resolve paths
sess.get("badlabel", "npy")
# → PosixPath('derivatives/preprocess/badlabel/session-pre/subject-01/...')

# Check existence
sess.have("badlabel", "npy")   # False (files don't exist yet)

# Bundle: all members of a group
sess.bundle("badlabel")
# → {'npy': PosixPath(...), 'featuremap': PosixPath(...)}
```

## 7. Use stages for multi-step fallback

When a pipeline has multiple processing stages and you want the "best available" output:

```python
# Get a stage handle
stage = ctx.stage("badlabel")

# Check if all members exist for a session
stage.have(sess)               # True/False

# Try stages in preference order, return first that exists on disk
stage.resolve(sess, prefer=["interpolate", "filter", "raw_zarr"])
```

## 8. Use from the registry (high-level)

Instead of loading configs manually, use the registry:

```python
from pipeio import PipelineContext

ctx = PipelineContext.from_registry("preprocess", flow="ieeg", root=Path("."))
ctx.groups()                   # ['badlabel', 'raw_zarr']
```

## What's next

- [How-to: Manage flows](../how-to/flows.md) — list, create, inspect flows
- [How-to: Resolve paths](../how-to/paths.md) — PipelineContext and Session patterns
- [Explanation: Three-level hierarchy](../explanation/hierarchy.md) — pipe / flow / mod
- [Reference: CLI commands](../reference/cli.md) — full command syntax
