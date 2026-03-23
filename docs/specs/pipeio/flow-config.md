# pipeio: Flow Config Specification

## Purpose

Each flow owns a `config.yml` that declares its inputs, outputs, and the **output registry** — a declarative data contract that both the workflow engine (Snakemake) and notebooks consume. pipeio loads, validates, and provides typed access to this config.

## Config Schema

Derived from pixecog's config.yml files (preprocess/ieeg, brainstate, sharpwaveripple):

```yaml
# -------------------------
# Pipeline inputs
# -------------------------
input_dir: "raw"                                    # root of input data
input_registry: "raw/registry.yml"                  # input registry YAML

# Secondary input sources (convention: input_dir_<name> + input_registry_<name>)
input_dir_brainstate: "derivatives"
input_registry_brainstate: "derivatives/brainstate/flow-brainstate_registry.yml"

# Workflow-engine-specific input spec (passed through, not validated by pipeio)
pybids_inputs:
  ieeg:
    filters:
      suffix: 'ieeg'
      extension: '.lfp'
      datatype: 'ieeg'
    wildcards:
      - subject
      - session
      - task

# -------------------------
# Member sets (YAML anchors for reuse)
# -------------------------
_member_sets:
  json_default: &json_default
    json: { suffix: "ieeg", extension: ".json" }
  lfp_default: &lfp_default
    lfp: { suffix: "ieeg", extension: ".lfp" }
  ieeg_bundle: &ieeg_bundle
    <<: [*json_default, *lfp_default]

# -------------------------
# Pipeline outputs
# -------------------------
output_dir: "derivatives/preprocess"
output_registry: "derivatives/preprocess/pipe-preprocess_flow-ieeg_registry.yml"

# -------------------------
# Output registry (the data contract)
# -------------------------
registry:
  raw_zarr:
    base_input: "ieeg"                # which pybids_input drives entity expansion
    bids:
      root: "raw_zarr"               # subdirectory under output_dir
      datatype: "ieeg"               # BIDS datatype
    members:
      zarr: { suffix: "ieeg", extension: ".zarr" }

  badlabel:
    base_input: "ieeg"
    bids:
      root: "badlabel"
      datatype: "ieeg"
    members:
      npy: { suffix: "ieeg", extension: ".npy" }
      featuremap: { suffix: "ieeg", extension: ".featuremap.png" }
```

## Schema Breakdown

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `input_dir` | string | yes | Root directory for input data (relative to project root) |
| `input_registry` | string | no | Path to the input registry YAML |
| `output_dir` | string | yes | Root directory for output data |
| `output_registry` | string | no | Path where the output registry will be written |
| `registry` | mapping | yes | The output registry — the data contract |
| `pybids_inputs` | mapping | pass-through | Workflow-engine-specific input specification |
| `_member_sets` | mapping | no | YAML anchor definitions for member reuse |

### Extra Input Sources

The convention `input_dir_<name>` + `input_registry_<name>` allows flows to declare secondary input sources from other pipelines' outputs. pipeio discovers these automatically by scanning config keys with the `input_dir_` prefix.

### Registry Group Schema

Each key under `registry:` is a **group** (also called a family or stage):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `base_input` | string | no | Which `pybids_inputs` key drives wildcard expansion |
| `bids` | mapping | no | BIDS directory structure (`root`, `datatype`) |
| `members` | mapping | yes | Named output products within this group |

Each member is a mapping with at minimum `suffix` and `extension`. Additional BIDS entity overrides (e.g., `recording: null`) are passed through.

## FlowConfig Python API

```python
from pipeio.config import FlowConfig

cfg = FlowConfig.from_yaml(Path("code/pipelines/preprocess/ieeg/config.yml"))

# Access core fields
cfg.input_dir       # "raw"
cfg.output_dir      # "derivatives/preprocess"
cfg.registry        # dict[str, RegistryGroup]

# List registry groups and members
list(cfg.registry.keys())                    # ['raw_zarr', 'badlabel', ...]
cfg.registry["badlabel"].members.keys()      # ['npy', 'featuremap']

# Pass-through fields (workflow-engine-specific)
cfg.extra["pybids_inputs"]                   # accessible but not validated by pipeio
```

### Pydantic Models

```python
class RegistryMember(BaseModel):
    suffix: str
    extension: str

class RegistryGroup(BaseModel):
    base_input: str | None = None
    bids: dict[str, str] = {}           # root, datatype, etc.
    members: dict[str, RegistryMember] = {}

class FlowConfig(BaseModel):
    input_dir: str = ""
    input_registry: str = ""
    output_dir: str = ""
    output_registry: str = ""
    registry: dict[str, RegistryGroup] = {}
    extra: dict[str, Any] = {}          # pass-through for engine-specific fields
```

## Validation Rules

1. `input_dir` must be non-empty
2. `output_dir` must be non-empty
3. Every registry group must have at least one member
4. Every member must have `suffix` and `extension`
5. `base_input` (if set) should reference a key in `pybids_inputs` (warning, not error — pipeio doesn't validate engine-specific fields)
6. No duplicate member names across groups (warning — valid but confusing)

## Output Registry Generation

pipeio can generate the output registry file (the `output_registry` path) by copying the `registry:` section from config.yml. This is currently done as a Snakemake rule in pixecog but can be a pipeio CLI command:

```
$ pipeio flow write-registry <pipe> <flow>
Written: derivatives/preprocess/pipe-preprocess_flow-ieeg_registry.yml
```

This registry file is then consumed by downstream pipelines via `input_registry_<name>` references.
