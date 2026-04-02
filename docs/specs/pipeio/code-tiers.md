# Code Tiers: Libraries, Utils, and Flow Scripts

## Overview

A projio project organizes code in three tiers. Each tier has a different scope, development model, and relationship to the pipeline system. pipeio's scaffolding and audit tools must be tier-aware.

```
core library (cogpy)          code/lib/cogpy/       dataset-agnostic, reusable
                                                     pure functions, no project deps
        ↑ promotes to                                 role: core in codio
        │
project utils (utils)         code/utils/            project-specific glue
                                                     PipelineContext, bootstrap, helpers
        ↑ promotes to                                 configured in projio config
        │
flow scripts                  code/pipelines/{flow}/scripts/
                                                     snakemake wiring, one per rule
                                                     imports from tiers above
        ↑ promotes from
        │
notebooks                     code/pipelines/{flow}/notebooks/explore/.src/
                                                     prototyping, investigation
```

## Tier Definitions

### Core Library

**Location:** `code/lib/{name}/` (one or more per project)

**Properties:**
- Dataset-agnostic: no project paths, no snakemake references, no registry lookups
- Pure functions: numpy/scipy/xarray in → same out
- Independently testable with synthetic data
- Versioned, may be published to PyPI
- Actively developed alongside the project pipelines

**Examples:** `cogpy.preprocess.filter_bandpass()`, `cogpy.spectral.compute_spectrogram()`

**codio role:** `core` — agents can add code here via `nb_promote` or manual extraction.

A project may have multiple core libraries (e.g., `cogpy` for signal processing, `vizlib` for plotting).

### Project Utils

**Location:** `code/utils/` (one per project)

**Properties:**
- Pipeline-architecture-aware: knows about PipelineContext, registry, BIDS paths, session entities
- Project-specific: references project conventions, data layouts, derivative structures
- Not independently publishable — coupled to the project
- Provides bootstrap helpers for notebooks and scripts
- May contain legacy code superseded by pipeio tools

**Examples:** `utils.io.PipelineContext`, `utils.io.notebook_bootstrap()`, `utils.io.brainstate.read_state_epochs()`

**projio config:** `code.project_utils: code/utils`

### Flow Scripts

**Location:** `code/pipelines/{flow}/scripts/{script}.py`

**Properties:**
- Snakemake wiring: `snakemake.input`, `snakemake.output`, `snakemake.params`
- Flow-specific: one script per rule (may be shared across rules within a flow)
- Imports from core library and project utils
- Not reusable across flows — if the same logic appears in two flows, it should be promoted

**pipeio tools:** `mod_create`, `script_create`, `nb_promote`

## Configuration

### projio config

```yaml
# .projio/config.yml
code:
  project_utils: code/utils          # project glue layer path
```

Core libraries are auto-discovered from `code/lib/*/` and registered in codio with `role: core`.

### codio catalog

```yaml
# .projio/codio/catalog.yml
libraries:
  cogpy:
    kind: internal
    role: core                       # actively developed, promote target
    runtime_import: cogpy
    source_path: code/lib/cogpy
  labbox:
    kind: internal
    role: shared                     # installed, not developed here
    runtime_import: labbox
    source_path: code/lib/labbox
```

**Role values:**
- `core` — project's own compute library. Agents can add functions here.
- `shared` — lab/org library, used as-is. Agents should not modify.
- `external` — PyPI package. Never modified.

## Tool Interactions

### Scaffolding (downward flow)

When pipeio scaffolds code, it queries projio config + codio to generate tier-aware templates:

```
┌─────────────┐    codio: role=core libraries    ┌──────────────────┐
│ pipeio      │ ──────────────────────────────→  │ import cogpy     │
│ nb_create   │    projio: project_utils path    │ from utils.io    │
│ script_create│ ──────────────────────────────→  │   import ...     │
│ mod_create  │                                   │                  │
└─────────────┘                                   └──────────────────┘
```

#### `nb_create` — kind-aware + tier-aware

**investigate/explore notebook:**
```python
# %% Setup
from pathlib import Path
import yaml

import cogpy                                    # ← core library (from codio, role=core)
from utils.io import PipelineContext            # ← project utils (from projio config)

config_path = Path("../../config.yml")
with open(config_path) as f:
    config = yaml.safe_load(f)
ctx = PipelineContext.from_registry("<flow>")   # ← project-specific bootstrap

# Available registry groups: deriv, qc, ...
```

**demo notebook:**
```python
# %% Setup
import cogpy                                    # ← core library
output_dir = Path("derivatives/<flow>")

# %% Load Outputs
# ...
```

#### `script_create` / `mod_create`

```python
"""Description"""
from pathlib import Path

import cogpy                    # ← core library (from codio)

def main(snakemake):
    # --- Inputs ---
    lfp = Path(snakemake.input.lfp)
    # --- Outputs ---
    out = Path(snakemake.output.out)
    # --- Processing ---
    result = cogpy.preprocess.filter_bandpass(...)  # ← agent fills this
    ...
```

### Promotion (upward flow)

When code moves between tiers, pipeio classifies it:

```
                 ┌─────────────────────────────────┐
notebook cell →  │ pipeio_code_classify / nb_promote│
                 └──────────┬──────────────────────┘
                            │
            ┌───────────────┼───────────────────┐
            ▼               ▼                   ▼
     core_library    project_utils        flow_script
     (cogpy)         (code/utils)         (scripts/)
```

#### Classification heuristic

| Signal in code | Tier | Reason |
|---------------|------|--------|
| References `snakemake.*` | flow_script | Coupled to snakemake execution |
| References `PipelineContext`, registry, project paths | project_utils | Pipeline-architecture-aware |
| Pure function: ndarray/xarray in → same out, no project deps | core_library | Dataset-agnostic, reusable |
| Uses project-specific constants, file formats, naming conventions | project_utils | Project-coupled but not flow-coupled |
| Hardcoded subject/session values | flow_script (or refactor) | Too specific for any library |

#### AST-based detection

```python
def classify_function(func_ast, project_utils_imports, core_lib_imports):
    names_used = extract_all_names(func_ast)

    if "snakemake" in names_used:
        return "flow_script"
    if names_used & project_utils_imports:  # PipelineContext, Session, etc.
        return "project_utils"
    if is_pure_function(func_ast):          # no global state, no IO side effects
        return "core_library"
    return "project_utils"                  # conservative default
```

### Audit (lateral / validation)

#### `mod_audit` enhancement — import health

Add to existing `mod_audit` checks:

- **Deprecated import:** Script imports `utils.io.PipelineRegistry` (superseded by pipeio)
- **Missing import:** Script imports `cogpy.preprocess.filter_v2` but function was renamed/removed
- **Tier violation:** Script in `code/lib/cogpy/` imports from `utils.io` (core library should not depend on project utils)

#### `script_audit` — new tool, cross-flow

```python
pipeio_script_audit(flow=None)  # None = all flows
```

Returns:
- **Duplicate functions:** Same function body in scripts across different flows
- **Common patterns:** Import patterns shared by 3+ scripts → candidate for utils/library
- **Promotion candidates:** Pure functions in flow scripts that belong in core library

## New Tools

### `pipeio_code_classify` (embedded in `nb_promote`)

Enhance `nb_promote` to return tier classification for each extracted function:

```python
result = pipeio_nb_promote(flow, name, mod)
# result now includes:
{
  "classifications": [
    {"name": "bandpass_filter", "tier": "core_library",
     "reason": "pure function, numpy in/out, no project deps",
     "suggested_module": "cogpy.preprocess"},
    {"name": "load_session_lfp", "tier": "project_utils",
     "reason": "uses PipelineContext",
     "suggested_module": "utils.io"},
    {"name": "run_pipeline", "tier": "flow_script",
     "reason": "references snakemake.input"}
  ]
}
```

### `pipeio_script_audit` (new)

Cross-flow script analysis:

```python
result = pipeio_script_audit()
{
  "duplicate_functions": [
    {"name": "load_lfp", "flows": ["preprocess_ieeg", "sharpwaveripple"],
     "similarity": 0.95, "suggestion": "promote to utils.io or cogpy.io"}
  ],
  "promotion_candidates": [
    {"name": "robust_zscore", "flow": "preprocess_ieeg", "script": "badlabel.py",
     "tier": "core_library", "reason": "pure function, used in 3 scripts"}
  ],
  "import_patterns": {
    "cogpy.preprocess": 12,     # used in 12 scripts
    "utils.io.Session": 8,     # used in 8 scripts
    "numpy": 25,               # universal
  }
}
```

### `mod_audit` enhancement — import checks

Add to existing `mod_audit`:

```python
# New check category: "import_health"
{
  "severity": "warning",
  "check": "import_health",
  "message": "Script badlabel.py imports utils.io.PipelineRegistry (deprecated, use pipeio)"
}
```

## projio responsibilities

### `projio init` / `projio sync` — auto-register libraries

When projio initializes or syncs a workspace:

1. Scan `code/lib/*/` for Python packages
2. For each, check if registered in codio catalog
3. If not, auto-register with:
   - `kind: internal`
   - `role: core` if path matches `code.core_library` config (or first found)
   - `role: shared` otherwise
   - `runtime_import: {package_name}`
4. Register `code/utils/` path in projio config if it exists

### `project_context()` enhancement

Include code tier info in project context response:

```json
{
  "code_tiers": {
    "core_libraries": [
      {"name": "cogpy", "import": "cogpy", "path": "code/lib/cogpy", "role": "core"}
    ],
    "project_utils": "code/utils",
    "flow_count": 9
  }
}
```

## Entity Management Matrix (updated)

| Entity | Create | Read | Update | Audit | Promote to |
|--------|--------|------|--------|-------|-----------|
| **Flow** | `flow_new` | `flow_list`, `flow_status` | agent edits | `registry_validate` | — |
| **Mod** | `mod_create` | `mod_list`, `mod_context` | agent edits | `mod_audit` | — |
| **Rule** | `rule_stub`, `rule_insert` | `rule_list` | `rule_update` | `mod_audit` | — |
| **Config** | `config_init` | `config_read` | `config_patch` | `contracts_validate` | — |
| **Script** | `script_create` | `mod_context` | agent edits | `mod_audit` + import checks | core_library, project_utils |
| **Notebook** | `nb_create` (tier-aware) | `nb_read`, `nb_analyze` | `nb_update` | `nb_audit` | flow_script via `nb_promote` |
| **Mod docs** | `mod_create` | `mod_context` | `mod_doc_refresh` | `mod_audit` | — |
| **Core library** | codio_add (via projio sync) | `codio_get`, `codio_func_doc` | agent edits | `script_audit` reverse-deps | — |
| **Project utils** | projio config | agent reads | agent edits | `mod_audit` import checks | core_library |

## Implementation Order

1. **projio config model** — add `code.project_utils` field, parse in `_load_config()`
2. **projio sync** — auto-discover `code/lib/*/`, register in codio with role
3. **codio catalog** — add `role` field to catalog schema
4. **pipeio nb_create** — query projio config for project_utils, generate tier-aware bootstrap
5. **pipeio script_create / mod_create** — include project_utils imports when relevant
6. **pipeio nb_promote** — add classification output (AST-based tier detection)
7. **pipeio mod_audit** — add import health checks (deprecated utils, missing functions)
8. **pipeio script_audit** — new tool, cross-flow duplicate/pattern detection
