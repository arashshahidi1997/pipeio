# pipeio: CLI Specification

## Entry Point

```
pipeio [-h] {init,flow,nb,registry,contracts} ...
```

Installed via `pip install pipeio`, entry point `pipeio = "pipeio.cli:main"`.

## Commands

### `pipeio init`

Scaffold `.pipeio/` in the current project.

```
pipeio init [--pipelines-dir PATH] [--root PATH]
```

Creates:

```
.pipeio/
├── registry.yml          # empty pipeline registry
└── templates/
    └── flow/             # default flow template
```

If `.projio/config.yml` exists, adds a `pipeio:` section.

### `pipeio flow`

Flow management.

```
pipeio flow list [--pipe PIPE]                          # list all flows
pipeio flow new <pipe> <flow>                           # scaffold a new flow
pipeio flow path <flow>                                 # print code directory path
pipeio flow config <flow>                               # print config path
pipeio flow deriv <flow>                                # print derivative directory path
pipeio flow status <flow>                               # show flow status + output summary
pipeio flow targets <flow> [-g GRP] [-m MEM] [-e k=v] [-x]  # resolve output paths
pipeio flow run <flow> [targets] [-c N] [-n] [-f k=v]  # launch via screen + wildcards
pipeio flow log <flow> [-n LINES]                       # tail latest run log
pipeio flow mods <flow>                                 # list mods and their rules
pipeio flow smk <flow> [snakemake args...]              # run snakemake in flow context
pipeio flow ids                                         # print flow names (completion)
```

### `pipeio nb`

Notebook lifecycle management. Requires `pipeio[notebook]`.

```
pipeio nb pair     [--config PATH] [--entry NAME]
pipeio nb sync     [--config PATH] [--entry NAME] [--dry]
pipeio nb exec     [--config PATH] [--entry NAME] [--timeout SECS]
pipeio nb publish  [--config PATH] [--entry NAME] [--format FORMAT]
pipeio nb status   [--config PATH]
pipeio nb new      --mode MODE NAME [--flow PIPE/FLOW]
```

Default config: `notebooks/notebook.yml` relative to CWD.

### `pipeio registry`

Pipeline registry management.

```
pipeio registry scan [--pipelines-dir PATH] [--output PATH]
pipeio registry validate [--registry PATH]
pipeio registry show [--pipe PIPE] [--format yaml|table]
```

### `pipeio contracts`

Pipeline I/O validation.

```
pipeio contracts validate <pipe/flow> [--stage inputs|outputs] [--contract PATH]
pipeio contracts list [--pipe PIPE]
```

## Common Options

| Option | Description |
|--------|-------------|
| `--root PATH` | Project root (default: auto-detect via `.projio/` or `.git`) |
| `--dry` | Dry run — show what would happen without executing |
| `--verbose` | Verbose output |
| `--quiet` | Suppress non-error output |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error / not implemented |
| 2 | Validation failure (contracts, registry) |

## Auto-Detection

When `--root` is not specified, pipeio walks up from CWD looking for:

1. `.pipeio/` directory
2. `.projio/config.yml` with a `pipeio:` section
3. `.git` directory (last resort)

When inside a flow directory (e.g., `code/pipelines/preprocess/ieeg/`), pipeio auto-detects the current pipe and flow from the path structure.
