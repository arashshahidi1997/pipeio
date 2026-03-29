# Authoring a Pipeline Step

This tutorial walks through creating a complete pipeline step — from config scaffolding to a working Snakemake rule with a wired script. You will use pipeio's authoring tools to generate structural plumbing so you only need to write the scientific logic.

## Prerequisites

- A project with pipeio initialized (`pipeio init`)
- At least one flow in the registry (`pipeio flow new <pipe> <flow>`)
- projio MCP server running (for MCP tool examples)

## Overview

The authoring chain is:

```
config_init → config_patch → mod_create → rule_insert → rule_update
```

Each tool generates one layer of the pipeline plumbing. By the end, you have a config, a script with I/O wiring, and a Snakemake rule — all consistent with each other.

## Scenario

We'll add a `badlabel` step to a `preprocess/ieeg` flow. This step takes raw LFP data, detects bad channels, and produces a labeled array and a feature visualization.

## 1. Scaffold the config

If your flow doesn't have a `config.yml` yet, scaffold one:

```python
pipeio_config_init(
    pipe="preprocess",
    flow="ieeg",
    input_dir="raw",
    output_dir="derivatives/preprocess",
    pybids_inputs={
        "ieeg": {
            "filters": {"suffix": "ieeg", "extension": ".lfp"},
            "wildcards": ["subject", "session", "task"],
        }
    },
)
```

This creates `config.yml` with:

```yaml
input_dir: raw
pybids_inputs:
  ieeg:
    filters:
      suffix: ieeg
      extension: .lfp
    wildcards:
    - subject
    - session
    - task
output_dir: derivatives/preprocess
output_registry: derivatives/preprocess/pipe-preprocess_flow-ieeg_registry.yml
registry: {}
```

The `output_registry` path follows the naming convention automatically.

> **Already have a config?** Skip this step — use `config_read` to inspect it and `config_patch` to modify it.

## 2. Add registry groups

Now add output groups for the pipeline step using `config_patch`:

```python
pipeio_config_patch(
    pipe="preprocess",
    flow="ieeg",
    registry_entry={
        "badlabel": {
            "base_input": "ieeg",
            "bids": {"root": "badlabel", "datatype": "ieeg"},
            "members": {
                "npy": {"suffix": "ieeg", "extension": ".npy"},
                "featuremap": {"suffix": "ieeg", "extension": ".featuremap.png"},
            },
        }
    },
    apply=True,
)
```

This validates the group schema (checks `base_input` references a known `pybids_inputs` key, checks member suffix/extension) and returns a diff before applying. Set `apply=True` to write.

## 3. Scaffold the mod script

Create a script with real I/O wiring:

```python
pipeio_mod_create(
    pipe="preprocess",
    flow="ieeg",
    mod="badlabel",
    description="Detect bad channels via feature-based labeling",
    inputs={
        "lfp": "raw LFP signal (.lfp)",
        "channels": "channel metadata (.tsv)",
    },
    outputs={
        "npy": "labeled channel array",
        "featuremap": "feature visualization",
    },
    params_spec={
        "threshold": "detection threshold (float)",
        "features": "list of feature names",
    },
)
```

This creates `scripts/badlabel.py`:

```python
"""Detect bad channels via feature-based labeling"""

from pathlib import Path


def main(snakemake):
    """Entry point called by Snakemake rule."""

    # --- Inputs ---
    lfp = Path(snakemake.input.lfp)  # raw LFP signal (.lfp)
    channels = Path(snakemake.input.channels)  # channel metadata (.tsv)

    # --- Outputs ---
    npy = Path(snakemake.output.npy)  # labeled channel array
    featuremap = Path(snakemake.output.featuremap)  # feature visualization

    # --- Parameters ---
    threshold = snakemake.params.threshold  # detection threshold (float)
    features = snakemake.params.features  # list of feature names

    # --- Processing (TODO: implement) ---
    npy.parent.mkdir(parents=True, exist_ok=True)
    pass


if __name__ == "__main__":
    main(snakemake)  # noqa: F821
```

It also creates `docs/mod-badlabel.md` with frontmatter.

The script has all the plumbing wired — inputs unpacked, outputs unpacked, params bound, output directory creation. You only need to replace the `pass` with your processing logic.

### With PipelineContext

If your script needs path resolution beyond what Snakemake provides:

```python
pipeio_mod_create(
    pipe="preprocess",
    flow="ieeg",
    mod="badlabel",
    description="Detect bad channels via feature-based labeling",
    inputs={"lfp": "raw LFP signal"},
    outputs={"npy": "labeled array"},
    use_pipeline_context=True,
)
```

This adds PipelineContext boilerplate to the script:

```python
from pipeio.resolver import PipelineContext

# --- Pipeline context ---
ctx = PipelineContext.from_config(Path(snakemake.params.config_path))
# session = ctx.session(sub=..., ses=...)
```

### Seeding from a notebook

If you've been prototyping in a notebook:

```python
pipeio_mod_create(
    pipe="preprocess",
    flow="ieeg",
    mod="badlabel",
    description="Detect bad channels via feature-based labeling",
    from_notebook="investigate_noise",
    inputs={"lfp": "raw LFP signal"},
    outputs={"npy": "labeled array"},
)
```

This analyzes the notebook and seeds the script with its imports.

## 4. Insert the rule

Now create and insert a Snakemake rule that references the script:

```python
pipeio_rule_insert(
    pipe="preprocess",
    flow="ieeg",
    rule_name="badlabel_detect",
    inputs={
        "lfp": {"source_rule": "raw_zarr", "member": "zarr"},
        "channels": {"source_rule": "raw_zarr", "member": "channels"},
    },
    outputs={
        "npy": {"root": "badlabel", "suffix": "ieeg", "extension": ".npy"},
        "featuremap": {"root": "badlabel", "suffix": "ieeg", "extension": ".featuremap.png"},
    },
    params={"threshold": "badlabel.threshold", "features": "badlabel.features"},
    script="scripts/badlabel.py",
    after_rule="raw_zarr",
)
```

This inserts into the Snakefile (or a matching `.smk` file):

```python
rule badlabel_detect:
    input:
        lfp=rules.raw_zarr.output.zarr,
        channels=rules.raw_zarr.output.channels,
    output:
        npy=bids(root="badlabel", suffix="ieeg", extension=".npy"),
        featuremap=bids(root="badlabel", suffix="ieeg", extension=".featuremap.png"),
    params:
        threshold=config["badlabel"]["threshold"],
        features=config["badlabel"]["features"],
    script:
        "scripts/badlabel.py"
```

### File auto-selection

If `target_file` is omitted, pipeio auto-selects:

- If `badlabel.smk` exists, the rule goes there (matched by mod prefix)
- Otherwise it goes into the main `Snakefile`

### Positioning

- `after_rule="raw_zarr"` places the rule right after the `raw_zarr` rule
- Omit `after_rule` to append at the end of the file

### Using pre-formatted text

If you already have rule text (e.g. from `rule_stub` or hand-written):

```python
pipeio_rule_insert(
    pipe="preprocess",
    flow="ieeg",
    rule_name="badlabel_detect",
    rule_text="rule badlabel_detect:\n    input:\n        ...",
)
```

## 5. Refine with rule_update

Need to add another input or parameter to an existing rule? Use `rule_update` instead of editing the file:

```python
pipeio_rule_update(
    pipe="preprocess",
    flow="ieeg",
    rule_name="badlabel_detect",
    add_inputs={
        "electrodes": {"source_rule": "raw_zarr", "member": "electrodes"},
    },
    add_params={
        "neighborhood": "badlabel.neighborhood_type",
    },
)
```

This returns a unified diff preview:

```diff
--- a/pipelines/preprocess/ieeg/Snakefile
+++ b/pipelines/preprocess/ieeg/Snakefile
@@ -10,6 +10,7 @@
     input:
         lfp=rules.raw_zarr.output.zarr,
         channels=rules.raw_zarr.output.channels,
+        electrodes=rules.raw_zarr.output.electrodes,
     output:
         ...
     params:
         threshold=config["badlabel"]["threshold"],
         features=config["badlabel"]["features"],
+        neighborhood=config["badlabel"]["neighborhood_type"],
```

Review the diff, then apply:

```python
pipeio_rule_update(
    pipe="preprocess",
    flow="ieeg",
    rule_name="badlabel_detect",
    add_inputs={"electrodes": {"source_rule": "raw_zarr", "member": "electrodes"}},
    add_params={"neighborhood": "badlabel.neighborhood_type"},
    apply=True,
)
```

Existing entries are never overwritten — only new names are added.

## 6. Validate

Check everything is consistent:

```python
pipeio_contracts_validate()
pipeio_registry_validate()
```

## Summary

| Step | Tool | What it creates |
|------|------|----------------|
| 1 | `config_init` | `config.yml` with dirs, pybids_inputs, empty registry |
| 2 | `config_patch` | Registry groups and params in config |
| 3 | `mod_create` | `scripts/<mod>.py` with I/O wiring + `docs/mod-<mod>.md` |
| 4 | `rule_insert` | Snakemake rule in the right file and position |
| 5 | `rule_update` | Incremental patches to existing rules |
| 6 | `contracts_validate` | Consistency check |

The key principle: **pipeio generates structural plumbing so you only write scientific logic.** The chain from config to rule is deterministic and validated at each step.

## What's next

- [Quickstart](quickstart.md) — project initialization and path resolution basics
- [How-to: Resolve paths](../how-to/paths.md) — PipelineContext and Session patterns
- [Explanation: Three-level hierarchy](../explanation/hierarchy.md) — pipe / flow / mod
- [Reference: MCP tools](../../specs/pipeio/mcp-tools.md) — full tool specification
