# Manage flows

## List all flows

```bash
pipeio flow list
pipeio flow list --pipe preprocess
```

Or via the shell helper:

```bash
pf                    # list all flows
pf ieeg               # cd into flow code directory
```

## Create a new flow

```bash
pipeio flow new preprocess ecephys
```

Creates a scaffold directory with `config.yml` and `Snakefile` under `code/pipelines/` or `pipelines/`.

## Inspect a flow

```bash
pipeio flow status ieeg
```

Shows config, Snakefile, mods, output groups, and file counts. Also available via MCP (`pipeio_flow_status`).

## Resolve output paths

```bash
pipeio flow targets ieeg                                    # show path patterns
pipeio flow targets ieeg -g badlabel -m npy                 # pattern for one member
pipeio flow targets ieeg -g badlabel -e sub=01 -e ses=04    # resolve concrete path
pipeio flow targets ieeg -g badlabel -x -e sub=01           # expand (glob) on disk
```

Also available via MCP: `pipeio_target_paths(pipe, flow, group, member, entities)`.

## Run a flow

```bash
pipeio flow run ieeg -c 4                                   # all targets, 4 cores
pipeio flow run ieeg -n                                     # dry run
pipeio flow run ieeg -f subject=01 -f session=04            # single session
```

Check progress and tail logs:

```bash
pipeio flow log ieeg             # tail latest run log
pipeio flow log ieeg -n 100      # last 100 lines
```

## List mods

```bash
pipeio flow mods ieeg
```

## Generate DAG

```bash
pipeio flow dag ieeg                   # SVG → docs/pipelines/ieeg/dag.svg
pipeio flow dag ieeg --format dot      # DOT to stdout (for piping)
pipeio flow dag ieeg --full            # full job DAG instead of rulegraph
```

DAGs are also auto-generated during `pipeio docs collect`.

Via MCP: `pipeio_dag_export(flow="ieeg", output_format="svg")` writes the SVG to the same location.

## Run snakemake directly

```bash
pipeio flow smk ieeg -n --reason
```

`pf <flow> smk` is a shortcut for the same.
