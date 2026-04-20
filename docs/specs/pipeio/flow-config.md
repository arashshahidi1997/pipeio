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
bids_dir: ""                                        # optional: snakebids scan root (defaults to input_dir)

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
| `bids_dir` | string | no | Scan root for `snakebids.generate_inputs` (defaults to `input_dir`) |
| `output_dir` | string | yes | Root directory for output data |
| `output_registry` | string | no | Path where the output registry will be written |
| `registry` | mapping | yes | The output registry — the data contract |
| `pybids_inputs` | mapping | pass-through | Workflow-engine-specific input specification |
| `_member_sets` | mapping | no | YAML anchor definitions for member reuse |

### Extra Input Sources

The convention `input_dir_<name>` + `input_registry_<name>` allows flows to declare secondary input sources from other pipelines' outputs. pipeio discovers these automatically by scanning config keys with the `input_dir_` prefix.

### `bids_dir` vs `input_dir`: scan root vs registry root

By default, `bids_dir` is empty and `snakebids.generate_inputs` scans `input_dir`. A downstream flow should override `bids_dir` when its upstream flow emits **multiple stages that share BIDS entities** — identical `suffix`, `extension`, and `datatype`, distinguished only by the stage subdirectory (`bids.root` in the upstream registry).

In that case, pointing `generate_inputs` at the parent `derivatives/<upstream>/` yields a "Multiple path templates" error, because pybids indexes files by parsed entities, not by directory. The fix is to decouple the two roots:

- **`bids_dir`** — narrow scan root (one stage subdirectory); used by `generate_inputs` for entity discovery
- **`input_dir`** — parent derivatives directory; used by `BidsPaths` for upstream registry lookups (e.g. cross-stage resolution via `stage.resolve`)

```yaml
# code/pipelines/spectrogram/config.yml — consumes spatial_median stage of preprocess_ieeg
input_dir: "derivatives/preprocess_ieeg"                    # parent — BidsPaths lookups
input_manifest: "derivatives/preprocess_ieeg/manifest.yml"
bids_dir: "derivatives/preprocess_ieeg/spatial_median"      # narrow — generate_inputs scan
```

```yaml
# code/pipelines/factor_analysis/config.yml — consumes the spectrogram stage
input_dir: "derivatives/spectrogram"
input_manifest: "derivatives/spectrogram/manifest.yml"
bids_dir: "derivatives/spectrogram/spectrogram"
```

The flow's `run.py` (or whatever invokes snakebids) reads `cfg["bids_dir"] or cfg["input_dir"]` for `generate_inputs`, and passes `cfg["input_dir"]` unchanged to `BidsPaths`. This keeps the BIDS filename shape untouched — no `desc-` retrofit needed on the upstream stage outputs — and preserves downstream group lookups.

When to leave `bids_dir` empty: single-stage upstream, or upstream stages already distinguished in filenames (e.g. each stage carries a distinct `desc-<label>` entity).

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
    bids_dir: str = ""                  # optional scan root for generate_inputs
    output_dir: str = ""
    output_registry: str = ""
    registry: dict[str, RegistryGroup] = {}
    extra: dict[str, Any] = {}          # pass-through for engine-specific fields

    def scan_dir(self) -> str:
        """Returns bids_dir if set, else input_dir — used as the snakebids scan root."""
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
