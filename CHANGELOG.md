# Changelog

## Unreleased

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
