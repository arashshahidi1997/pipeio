# Flow / Mod Hierarchy

Pipeio organizes computational pipelines around **flows** — the primary unit of organization.

## Flow

A **flow** is a self-contained snakebids app producing one derivative directory. It owns:

- **Snakefile** or workflow entry point
- **config.yml** declaring inputs, outputs, and the output registry
- **Output directory** under `derivatives/`
- Optional **notebooks/** for analysis and visualization

Examples: `ieeg`, `ecephys`, `brainstate`

Each flow is categorized by a `pipe` tag but stands alone as a deployable app. Pipeio's `get()` method can auto-select flows when unambiguous.

## Pipe (category tag)

A **pipe** is a category tag grouping related flows by scientific domain (e.g. `preprocess`, `spectral`). It is not a hierarchical container — the pipe/flow nesting is being flattened. Pipes exist as metadata for filtering and organization.

## Mod

A **mod** (logical module) is a group of related rules within a flow. Mods are identified by rule-name prefixes in the Snakefile.

Examples: `badchannel`, `linenoise`, `interpolate` (within `preprocess/ieeg`)

Mods are optional metadata — not every flow needs to enumerate them.

## Filesystem mapping

```
code/pipelines/
├── preprocess/              # pipe
│   ├── ieeg/                # flow
│   │   ├── Snakefile
│   │   ├── config.yml
│   │   └── notebooks/
│   └── ecephys/             # flow
│       ├── Snakefile
│       └── config.yml
└── brainstate/              # pipe (single flow)
    ├── Snakefile
    └── config.yml
```

## Registry representation

The `.pipeio/registry.yml` maps this hierarchy to metadata:

```yaml
flows:
  preprocess/ieeg:
    name: ieeg
    pipe: preprocess
    code_path: code/pipelines/preprocess/ieeg
    config_path: code/pipelines/preprocess/ieeg/config.yml
  brainstate:
    name: brainstate
    pipe: brainstate
    code_path: code/pipelines/brainstate
```

## Flow resolution

When querying by pipe name alone, pipeio resolves the flow:

1. If the pipe has exactly one flow → auto-select it
2. If a flow has the same name as the pipe → use it
3. Otherwise → require explicit flow name
