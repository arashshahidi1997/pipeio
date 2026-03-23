# Registry model

## Why a registry?

Research projects accumulate pipelines over time. Without a central index, discovering what pipelines exist — their configs, outputs, and documentation — requires manual filesystem exploration. The registry provides a machine-readable manifest.

## Two kinds of registry

### Pipeline registry (`.pipeio/registry.yml`)

Maps the pipe/flow/mod hierarchy to filesystem paths. Created by `pipeio registry scan`, consumed by `pipeio flow list` and `PipelineContext.from_registry()`.

```yaml
flows:
  preprocess/ieeg:
    name: ieeg
    pipe: preprocess
    code_path: code/pipelines/preprocess/ieeg
    config_path: code/pipelines/preprocess/ieeg/config.yml
```

### Output registry (per-flow `config.yml`)

Declares what a flow produces — groups of named output products. This is the **data contract** that both workflow engines and notebooks consume.

```yaml
registry:
  badlabel:
    members:
      npy: { suffix: "ieeg", extension: ".npy" }
      featuremap: { suffix: "ieeg", extension: ".featuremap.png" }
```

## Scan vs. manual

The pipeline registry can be generated automatically via `pipeio registry scan` or maintained manually. Scanning discovers flows from `Snakefile` / `config.yml` presence; manual editing allows adding metadata (mods, doc_paths) that scanning cannot infer.

## Validation

`pipeio registry validate` checks:

- Slug naming conventions (`^[a-z][a-z0-9_]*$`)
- Path existence (code directories, config files)
- ID uniqueness
- Documentation coverage (warning, not error)

This ensures the registry stays consistent as the project evolves.
