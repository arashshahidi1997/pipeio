# CLI commands

## Entry point

```
pipeio [-h] [--root PATH] {init,flow,nb,registry,contracts} ...
```

Installed via `pip install pipeio`. Entry point: `pipeio = "pipeio.cli:main"`.

## `pipeio init`

Scaffold `.pipeio/` in the current project.

```bash
pipeio init [--root PATH]
```

Creates `.pipeio/registry.yml` (empty) and `.pipeio/templates/flow/`.

## `pipeio flow list`

List all registered flows.

```bash
pipeio flow list [--pipe PIPE] [--root PATH]
```

## `pipeio flow new`

Scaffold a new flow directory.

```bash
pipeio flow new <pipe> <flow> [--root PATH]
```

Creates `<pipelines_dir>/<pipe>/<flow>/` with `config.yml` and `Snakefile`.

## `pipeio registry scan`

Discover flows from the filesystem and write the registry.

```bash
pipeio registry scan [--pipelines-dir PATH] [--output PATH] [--root PATH]
```

Walks the pipelines directory for `Snakefile` / `config.yml` presence. Writes to `.pipeio/registry.yml` by default.

## `pipeio registry validate`

Validate registry consistency.

```bash
pipeio registry validate [--registry PATH] [--root PATH]
```

Checks slug compliance, path existence, and ID uniqueness. Exit code 0 = valid, 2 = errors.

## `pipeio nb` (requires `pipeio[notebook]`)

Notebook lifecycle management. Not yet implemented.

```bash
pipeio nb pair|sync|exec|publish|status
```

## `pipeio contracts validate`

Pipeline I/O validation. Not yet implemented.

```bash
pipeio contracts validate
```

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
