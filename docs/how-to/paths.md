# Resolve paths

## From a config file

```python
from pathlib import Path
from pipeio import FlowConfig, PipelineContext

cfg = FlowConfig.from_yaml(Path("code/pipelines/preprocess/ieeg/config.yml"))
ctx = PipelineContext.from_config(cfg, root=Path("."))
```

## From the registry

```python
from pipeio import PipelineContext

ctx = PipelineContext.from_registry("preprocess", flow="ieeg", root=Path("."))
```

## Sessions

Bind entity values so every subsequent call doesn't repeat them:

```python
sess = ctx.session(subject="01", session="pre", task="rest")

# Resolve a path
path = sess.get("badlabel", "npy")

# Check existence
exists = sess.have("badlabel", "npy")

# Override a bound entity
path = sess.get("badlabel", "npy", task="active")

# All members of a group
paths = sess.bundle("badlabel")
# → {'npy': Path(...), 'featuremap': Path(...)}
```

## Stages

A stage handle wraps a registry group with existence checks:

```python
stage = ctx.stage("badlabel")

# All paths for a session
stage.paths(sess)

# Subset of members
stage.paths(sess, members=["npy"])

# Do all member files exist?
stage.have(sess)
```

## Multi-stage fallback

Try stages in order and return the first one whose files exist:

```python
stage = ctx.stage("badlabel")
winner = stage.resolve(sess, prefer=["interpolate", "filter", "raw_zarr"])
```

## Stage aliases

Define aliases in config.yml under `stage_aliases`:

```yaml
stage_aliases:
  labels: badlabel
  cleaned: interpolate
```

Then use the alias:

```python
stage = ctx.stage("labels")  # resolves to "badlabel"
```

## Extra input sources

Discover secondary input directories from config:

```python
cfg = FlowConfig.from_yaml(config_path)
extras = cfg.extra_inputs()
# → {'brainstate': ('derivatives', 'derivatives/brainstate/registry.yml')}
```
