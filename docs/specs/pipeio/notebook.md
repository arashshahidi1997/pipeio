# pipeio: Notebook Lifecycle Specification

## Purpose

Research pipeline flows produce notebooks that serve two roles:

1. **Exploratory** — investigate approaches, test parameters, validate ideas before promoting into pipeline scripts
2. **Demo/report** — show pipeline-produced artifacts, generate QC reports

pipeio manages the notebook lifecycle: pair, sync, execute, publish. This replaces the ~350 lines of Makefile shell scripts currently duplicated across every flow.

## Notebook Directory Convention

Each notebook lives in its own subdirectory within the flow's `notebooks/` directory:

```
code/pipelines/preprocess/ieeg/
└── notebooks/
    ├── notebook.yml                              # lifecycle config
    ├── investigate_noise_characterization_demo/
    │   ├── investigate_noise_characterization_demo.py    # source of truth
    │   ├── investigate_noise_characterization_demo.ipynb  # generated (paired)
    │   └── investigate_noise_characterization_demo.md     # generated (myst)
    └── investigate_noise_tfspace_demo/
        ├── investigate_noise_tfspace_demo.py
        ├── investigate_noise_tfspace_demo.ipynb
        └── investigate_noise_tfspace_demo.md
```

### Source of Truth

The `.py` file (percent format) is always the source of truth. The `.ipynb` and `.md` files are generated/synced artifacts.

### Notebook Header Convention

Notebook `.py` files carry structured docstring metadata:

```python
# ---
# Title: investigate_noise_characterization_demo.py
# Status: INVESTIGATION
# Objective: Prototype a compact cross-session noise-characterization demo
# Focus: PSD-first spectral characterization, spatial structure analysis
# Guardrails: Read-only, in-memory exploration only, flow-aware paths
# ---
```

## notebook.yml Schema

```yaml
publish:
  docs_dir: /abs/path/to/docs/reports/.../notebooks   # where to publish
  prefix: nb-                                           # filename prefix for published copies

entries:
  - path: notebooks/investigate_noise_characterization_demo/investigate_noise_characterization_demo.py
    pair_ipynb: true          # create .ipynb and pair with jupytext
    pair_myst: true           # create .md (MyST) and pair
    publish_myst: true        # copy .md to docs_dir after execution
    publish_html: false       # render HTML and copy to docs_dir
  - path: notebooks/investigate_noise_tfspace_demo/investigate_noise_tfspace_demo.py
    pair_ipynb: true
    pair_myst: true
    publish_myst: true
```

### NotebookConfig Pydantic Model

```python
class NotebookEntry(BaseModel):
    path: str
    pair_ipynb: bool = False
    pair_myst: bool = False
    publish_myst: bool = False
    publish_html: bool = False

class PublishConfig(BaseModel):
    docs_dir: str = ""
    prefix: str = "nb-"

class NotebookConfig(BaseModel):
    publish: PublishConfig = PublishConfig()
    entries: list[NotebookEntry] = []
```

## Lifecycle Stages

### 1. Pair (`pipeio nb pair`)

Create paired formats according to `notebook.yml`:

- If `pair_ipynb: true` and `.ipynb` doesn't exist → create it with `jupytext --to notebook`
- Set pairing formats with `jupytext --set-formats ipynb,py`
- If `pair_myst: true` → additionally set formats to `ipynb,py,md:myst`

**Idempotent**: skips if pairing already exists.

### 2. Sync (`pipeio nb sync`)

Synchronize content between paired formats using "newer wins" logic:

- Compare modification times of `.py`, `.ipynb`, `.md`
- Sync newer → older with `jupytext --sync`
- Directional: `.py` → `.ipynb` preserves ipynb outputs; `.ipynb` → `.py` strips outputs

**Safety**: never overwrites newer content with older. Dry-run mode (`--dry`) shows what would happen.

### 3. Execute (`pipeio nb exec`)

Execute all registered `.ipynb` notebooks in place:

- `jupyter nbconvert --to notebook --execute --inplace <path>`
- Respects timeout settings (configurable, default 600s)
- Reports success/failure per notebook
- Can target a single entry: `pipeio nb exec --entry <name>`

### 4. Publish (`pipeio nb publish`)

Copy executed notebooks to the documentation directory:

- If `publish_myst: true` → copy `.md` to `docs_dir/` with prefix
- If `publish_html: true` → render `.ipynb` to HTML, copy to `docs_dir/`
- Can also publish `.ipynb` directly for embedding

**Path construction**: `{docs_dir}/{prefix}{notebook_name}.{ext}`

### 5. Status (`pipeio nb status`)

Show sync and publication state:

```
$ pipeio nb status
Flow: preprocess/ieeg (2 entries)

  investigate_noise_characterization_demo
    .py   2026-03-20 14:30  (source)
    .ipynb 2026-03-20 14:28  ⚠ out of sync (py is newer)
    .md   2026-03-20 14:28  ⚠ out of sync
    published: no

  investigate_noise_tfspace_demo
    .py   2026-03-19 10:00  (source)
    .ipynb 2026-03-19 10:00  ✓ synced
    .md   2026-03-19 10:00  ✓ synced
    published: yes (2026-03-19)
```

## Full Pipeline Shortcut

`pipeio nb publish` (without sub-stage) runs the full pipeline:

```
pair → sync → exec → publish
```

Equivalent to the pixecog Makefile target `nb-publish`.

## CLI Commands

```
pipeio nb pair     [--config notebook.yml] [--entry NAME]
pipeio nb sync     [--config notebook.yml] [--entry NAME] [--dry]
pipeio nb exec     [--config notebook.yml] [--entry NAME] [--timeout 600]
pipeio nb publish  [--config notebook.yml] [--entry NAME] [--format myst|html|ipynb]
pipeio nb status   [--config notebook.yml]
pipeio nb new      --mode explore|demo [--flow PIPE/FLOW] NAME
```

## `pipeio nb new`

Scaffold a new notebook:

- `--mode explore` — minimal bootstrap, no publish defaults, structured docstring header
- `--mode demo` — publish-ready, wired to pipeline outputs, auto-registered in `notebook.yml`

Creates `notebooks/<name>/<name>.py` with the standard header and PipelineContext bootstrap code.

## Reference: pixecog Makefile Targets

The current implementation these specs replace:

| Make target | pipeio equivalent | Lines of bash |
|-------------|-------------------|---------------|
| `nb-list` | `pipeio nb status` | 3 |
| `nb-pair` | `pipeio nb pair` | 45 |
| `nb-status` | `pipeio nb sync --dry` | 30 |
| `nb-sync` | `pipeio nb sync` | 50 |
| `nb-exec-all` | `pipeio nb exec` | 25 |
| `nb-publish-myst` | `pipeio nb publish --format myst` | 40 |
| `nb-publish-html` | `pipeio nb publish --format html` | 35 |
| `nb-publish-ipynb` | `pipeio nb publish --format ipynb` | 30 |
| `nb-publish` | `pipeio nb publish` | 15 (orchestration) |

Total: ~350 lines of bash → Python CLI with proper error handling, progress reporting, and dry-run support.
