# CLI commands

## Entry point

```
pipeio [-h] [--root PATH] {init,flow,nb,registry,docs,contracts} ...
```

Installed via `pip install pipeio`. Entry point: `pipeio = "pipeio.cli:main"`.

## `pipeio init`

Scaffold `.pipeio/` in the current project.

```bash
pipeio init [--root PATH]
```

Creates `.pipeio/registry.yml` (empty) and `.pipeio/templates/flow/`.

## `pipeio flow`

Flow management commands.

### `pipeio flow list`

List all registered flows.

```bash
pipeio flow list [--pipe PIPE]
```

### `pipeio flow new`

Scaffold a new flow directory.

```bash
pipeio flow new <pipe> <flow>
```

Creates `<pipelines_dir>/<pipe>/<flow>/` with `config.yml` and `Snakefile`.

### `pipeio flow path`

Print the absolute code directory path for a flow.

```bash
pipeio flow path <flow>
```

### `pipeio flow config`

Print the absolute config path for a flow.

```bash
pipeio flow config <flow>
```

### `pipeio flow deriv`

Print the absolute derivative directory path for a flow.

```bash
pipeio flow deriv <flow>
```

### `pipeio flow status`

Show flow status: config, Snakefile, mods, output groups with file counts.

```bash
pipeio flow status <flow>
```

### `pipeio flow targets`

Resolve output paths for a flow's registry entries. Three modes:

- **Patterns** (default): show path templates for all groups/members
- **Resolve** (`-e key=val`): resolve a single concrete path
- **Expand** (`-x -e key=val`): glob for all matching paths on disk

```bash
pipeio flow targets <flow>                              # list all patterns
pipeio flow targets <flow> -g preproc -m cleaned        # pattern for one member
pipeio flow targets <flow> -g preproc -e sub=01 -e ses=04  # resolve single path
pipeio flow targets <flow> -g preproc -x -e sub=01      # expand (glob) on disk
```

### `pipeio flow run`

Launch Snakemake in a detached screen session with optional wildcard filtering.

```bash
pipeio flow run <flow> [targets...] [-c CORES] [-n] [-f key=val]
```

| Option | Description |
|--------|-------------|
| `-c, --cores N` | Number of cores (default 1) |
| `-n, --dryrun` | Dry run |
| `-f, --filter key=val` | Wildcard filter (repeatable), maps to snakebids `--filter-{key} {val}` |

### `pipeio flow log`

Tail the latest run log for a flow.

```bash
pipeio flow log <flow> [-n LINES]
```

### `pipeio flow mods`

List mods for a flow with their rules.

```bash
pipeio flow mods <flow>
```

### `pipeio flow dag`

Generate a DAG SVG for a flow and write it to `docs/pipelines/<flow>/dag.svg`.

```bash
pipeio flow dag <flow>                   # rulegraph SVG (default)
pipeio flow dag <flow> --format dot      # raw DOT to stdout (for piping)
pipeio flow dag <flow> --full            # full job DAG instead of rulegraph
```

Requires snakemake and graphviz (`dot`).

### `pipeio flow smk`

Run snakemake directly in a flow's context (resolves `--snakefile` and `--directory`).

```bash
pipeio flow smk <flow> [snakemake args...]
```

## `pipeio registry scan`

Discover flows from the filesystem and write the registry.

```bash
pipeio registry scan [--pipelines-dir PATH] [--output PATH]
```

Walks the pipelines directory for `Snakefile` / `config.yml` presence. Writes to `.pipeio/registry.yml` by default.

## `pipeio registry validate`

Validate registry consistency.

```bash
pipeio registry validate [--registry PATH]
```

Checks slug compliance, path existence, and ID uniqueness. Exit code 0 = valid, 2 = errors.

## `pipeio nb` (requires `pipeio[notebook]`)

Notebook lifecycle management.

```bash
pipeio nb pair [--force]    # pair .py notebooks with ipynb/myst
pipeio nb sync              # sync notebook formats
pipeio nb exec              # execute notebooks
pipeio nb publish           # publish notebooks to docs
pipeio nb status            # show notebook sync status
```

## `pipeio docs`

Pipeline documentation.

```bash
pipeio docs collect         # collect flow docs and notebook outputs into docs/pipelines/
pipeio docs nav             # generate MkDocs nav fragment
```

## `pipeio contracts validate`

Validate pipeline I/O contracts.

```bash
pipeio contracts validate
```

## Shell helper: `pf`

Source `bin/pf.sh` in your shell for quick flow navigation:

```bash
source /path/to/pipeio/bin/pf.sh
```

| Command | Action |
|---------|--------|
| `pf` | list all flows |
| `pf <flow>` | cd into flow code directory |
| `pf <flow> smk [args]` | run snakemake in flow context |
| `pf <flow> path` | print code directory path |
| `pf <flow> config` | print config path |
| `pf <flow> deriv` | cd into derivative directory |
| `pf <flow> status` | show flow status and output summary |
| `pf <flow> targets [opts]` | resolve output paths |
| `pf <flow> run [opts]` | launch snakemake in screen session |
| `pf <flow> log [-n N]` | tail latest run log |
| `pf <flow> mods` | list mods and their rules |
| `pf <flow> dag` | generate DAG SVG to docs |

Tab completion is provided for both bash and zsh.

## Common options

| Option | Description |
|--------|-------------|
| `--root PATH` | Project root (auto-detected if omitted) |

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error / not implemented |
| 2 | Validation failure |
