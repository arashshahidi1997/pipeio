# Workspace layout

## `.pipeio/` directory

Created by `pipeio init`:

```
.pipeio/
├── registry.yml          # pipeline registry (pipe/flow/mod → paths)
└── templates/
    └── flow/             # templates for pipeio flow new
```

## Flow directory

Created by `pipeio flow new` or manually:

```
code/pipelines/<pipe>/<flow>/
├── Snakefile             # workflow entry point
├── config.yml            # flow configuration + output registry
├── contracts.py          # optional: input/output validation
└── notebooks/
    ├── notebook.yml      # notebook configuration
    └── *.py              # Jupytext percent-format notebooks
```

## Typical project structure

```
my-project/
├── .pipeio/
│   └── registry.yml
├── code/pipelines/
│   ├── preprocess/
│   │   ├── ieeg/
│   │   │   ├── Snakefile
│   │   │   ├── config.yml
│   │   │   └── notebooks/
│   │   └── ecephys/
│   │       ├── Snakefile
│   │       └── config.yml
│   └── brainstate/
│       ├── Snakefile
│       └── config.yml
├── raw/                  # input data
└── derivatives/          # pipeline outputs
    ├── preprocess/
    │   ├── raw_zarr/
    │   └── badlabel/
    └── brainstate/
```
