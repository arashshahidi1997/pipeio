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
pipeio flow list [--pipe PIPE]                    # list all flows
pipeio flow new <pipe> <flow> [--template PATH]   # scaffold a new flow
pipeio flow status <pipe> <flow>                  # show flow status
pipeio flow write-registry <pipe> <flow>          # generate output registry file
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
