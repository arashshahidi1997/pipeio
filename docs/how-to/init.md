# Initialize a project

## Scaffold the workspace

```bash
pipeio init
```

This creates `.pipeio/` with an empty registry and default templates.

If you specify `--root`:

```bash
pipeio init --root /path/to/project
```

pipeio will scaffold at that location instead of auto-detecting from CWD.

## What gets created

```
.pipeio/
├── registry.yml          # empty pipeline registry (flows: {})
└── templates/
    └── flow/             # default flow template directory
```

## Auto-detection

When `--root` is not specified, pipeio walks up from the current directory looking for:

1. `.pipeio/` directory (already initialized)
2. `.projio/config.yml` with a `pipeio:` section
3. `.git` directory (last resort — uses the repo root)

## Idempotency

Running `pipeio init` when `.pipeio/` already exists is safe — it prints a message and exits without modifying anything.
