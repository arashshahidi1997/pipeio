# Changelog

## Unreleased

### Added
- **`mcp_nb_report`** — extract figures, markdown, and text outputs from an executed notebook; backend-aware (percent via nbconvert, marimo via `marimo export md`); detects interactive HTML widgets (holoviews, bokeh, plotly) and returns `html_outputs` with remediation hints; saves extracted figures to `{flow}/docs/reports/{name}/`
- **`mcp_nb_move`** — move a notebook between flows atomically (files + registry update)

### Fixed
- **`mcp_nb_exec` output placed in `.src/`** — executed notebook now overwrites the workspace `.ipynb` via `_nb_output_paths` instead of creating `_executed.ipynb` inside `.src/`
- **Duplicate `"flow"` keys** in `mcp_nb_exec` and `mcp_mod_list` return dicts
- **Stale `papermill_python` reference** in `mcp_nb_exec` error message
- **Resolver test assertions** — tests used long-form entity names (`subject-01`) instead of BIDS abbreviations (`sub-01`)

### Changed
- **Two-phase `docs_collect` architecture** — separated artifact generation (export) from collection. Export phase generates DAG SVGs and notebook HTML into `{flow}/.build/`; collect phase copies pre-built artifacts to `docs/pipelines/`. Collectors (`NotebookCollector`, `DagCollector`) are now pure file copiers — no inline nbconvert or snakemake calls. Pass `export=False` (or `--no-export` CLI flag) to skip generation when artifacts are pre-built.
- **`.build/` convention** — per-flow build directory (`code/pipelines/{flow}/.build/`) for exported artifacts (DAG SVGs, notebook HTML). Gitignored. MCP tools (`pipeio_dag_export`, `pipeio_nb_publish`) now write to `.build/` in addition to `docs/pipelines/` for immediate visibility.
- **`docs/pipelines/` is now a build artifact (gitignored)** — source of truth for hand-written docs is `code/pipelines/<flow>/docs/`. Collected files now carry source-path headers (`<!-- DO NOT EDIT — Source: ... -->`). Auto-generated files (index stubs, scripts.md, report.md, notebook index) carry `<!-- AUTO-GENERATED -->` headers. Prevents agents and humans from editing the wrong copy.

### Fixed
- **`overview.md` silently dropped** — when both `index.md` and `overview.md` existed in a flow's docs, overview content was lost due to an `_is_stale` mtime race. Now: overview.md is used as the flow index only when no source index.md exists; when both exist, both are collected as separate pages.
- **modkey.bib default output** — changed from `docs/pipelines/modkey.bib` to `.projio/pipeio/modkey.bib` to keep generated files out of `docs_dir` (prevents mkdocs false-positive citation warnings)

### Added (conventions)
- **Flow Overview convention** — `docs/overview.md` per flow: Purpose, Input/Output, Mod Chain, Design Decisions, Known Gaps. Scaffolded by `flow_new`, collected by `docs_collect`. See `docs/specs/pipeio/pipeline-docs.md`.
- **Pipeline Architecture convention** — `code/pipelines/architecture.md` per project: mermaid diagram, flow table, data flow chains, design principles. New `pipeio_architecture_init` tool scaffolds from registry + cross_flow. Collected by `docs_collect`, placed first in pipelines nav.

### Added
- **`pipeio_target_paths`** MCP tool — resolve output paths via PipelineContext (list/resolve/expand modes)
- **`pipeio flow dag`** CLI command — generate DAG SVG to `docs/pipelines/<flow>/dag.svg`
- **`pipeio flow status`** CLI — show flow config, mods, output groups with file counts
- **`pipeio flow targets`** CLI — resolve output paths from command line (`-g`, `-m`, `-e`, `-x`)
- **`pipeio flow run`** CLI — launch snakemake via screen with `--filter key=val` wildcard scoping
- **`pipeio flow log`** CLI — tail latest run log for a flow
- **`pipeio flow mods`** CLI — list mods and their rules
- **`pf.sh`** shell helper updated with all new subcommands + bash/zsh completion
- `docs_collect` auto-generates DAG SVG per flow, flow `index.md`, and top-level `docs/pipelines/index.md`
- `docs_nav` writes `docs/pipelines/mkdocs.yml` for mkdocs-monorepo-plugin integration
- `mcp_dag_export` SVG format auto-writes to `docs/pipelines/<flow>/dag.svg`
- `mcp_run` accepts `wildcards` dict for `--filter-{key} {value}` scoping (snakebids)
- `mcp_run` uses `stdbuf -oL` for unbuffered stdout under conda run
- `mcp_run_status` reports `log_bytes`, shows hint when log is empty, checks snakemake internal logs
- `mcp_config_read` returns `resolved_patterns` alongside `bids_signatures`

### Changed
- CLI refactored: extracted `_resolve_flow`, `_load_registry`, `_flow_code_dir`, `_flow_config_path` helpers — eliminated ~100 lines of duplicated flow-lookup logic
- `docs_nav` now generates monorepo sub-mkdocs.yml instead of a YAML fragment for manual patching
- `pipeio_mkdocs_nav_patch` simplified to write sub-mkdocs.yml + ensure `projio sync` configured the root mkdocs.yml

### Removed
- **`pipeio_dag`** MCP tool (static Snakefile parser) — redundant with `pipeio_dag_export` which uses snakemake's native graph output
