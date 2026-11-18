# Resolve paths

## From the CLI

```bash
pipeio flow targets ieeg                                    # show path patterns
pipeio flow targets ieeg -g badlabel -m npy -e sub=01       # resolve concrete path
pipeio flow targets ieeg -g badlabel -x -e sub=01           # expand (glob) on disk
```

## Via MCP

```
pipeio_target_paths(flow="preprocess_ieeg")                                      # list groups
pipeio_target_paths(flow="preprocess_ieeg", group="badlabel", member="npy",
                    entities={"subject": "01", "session": "04"})                  # resolve
pipeio_target_paths(flow="preprocess_ieeg", group="badlabel", expand=True,
                    entities={"subject": "01"})                                   # expand
```

Paths are built by the same code as the flow's `BidsPaths` (`pipeio.bidspath`),
under the bids spec the Snakefile sets with `set_bids_spec(...)`: the family's
`bids.root` comes first, then `sub-`/`ses-` directories, the datatype and the
filename. A resolved path is therefore a target snakemake can build. Wildcards
the entities leave open are listed in `missing_entities`.

Families the Snakefile adds to the registry in Python (one per band, per
detection, ...) are not in `config.yml`. When `group` is not found there, the
tool loads the Snakefile in the project env (`python_bin`, auto-resolved by
projio) with `snakebids.generate_inputs` stubbed so the dataset is not scanned,
and uses the registry of the flow's `BidsPaths`. The first load takes tens of
seconds; the result is cached in `.projio/pipeio/cache/registry/<flow>.json`
until a flow file changes. Pass `snakefile=True` to list those families, or
`snakefile=False` to stay on `config.yml`.

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
