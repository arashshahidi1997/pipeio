# Scan and validate the registry

## Scan for flows

Discover all pipeline flows from the filesystem:

```bash
pipeio registry scan
```

This walks the pipelines directory looking for `Snakefile` or `config.yml` files, then writes the discovered flows to `.pipeio/registry.yml`.

### Custom pipelines directory

```bash
pipeio registry scan --pipelines-dir code/pipelines
```

### Custom output path

```bash
pipeio registry scan --output custom/registry.yml
```

## Validate the registry

Check that all paths exist and names follow conventions:

```bash
pipeio registry validate
```

Validation checks:

- **Slug compliance** — names match `^[a-z][a-z0-9_]*$`
- **Code path existence** — flow directories exist on disk
- **Config path existence** — referenced config.yml files exist
- **ID uniqueness** — no duplicate pipe/flow combinations

Exit code 0 means valid, exit code 2 means validation errors found.

## Programmatic validation

```python
from pipeio import PipelineRegistry
from pathlib import Path

registry = PipelineRegistry.from_yaml(Path(".pipeio/registry.yml"))
result = registry.validate(root=Path("."))

if result.ok:
    print("Registry is valid")
else:
    for error in result.errors:
        print(f"ERROR: {error}")
    for warning in result.warnings:
        print(f"WARN: {warning}")
```
