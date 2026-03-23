# Three-level hierarchy

Pipeio organizes computational pipelines in a **pipe / flow / mod** hierarchy.

## Pipe

A **pipe** represents a scientific domain — a broad area of analysis.

Examples: `preprocess`, `brainstate`, `sharpwaveripple`, `spectral`

A pipe groups related flows that share a domain but may use different approaches or target different data types.

## Flow

A **flow** is a concrete workflow with its own:

- **Snakefile** or workflow entry point
- **config.yml** declaring inputs, outputs, and the output registry
- **Output directory** under `derivatives/`
- Optional **notebooks/** for analysis and visualization

Examples: `preprocess/ieeg`, `preprocess/ecephys`, `brainstate/brainstate`

When a pipe has only one flow, the flow often shares the pipe's name (e.g., `brainstate/brainstate`). Pipeio's `get()` method auto-selects in this case.

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
