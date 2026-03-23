# pipeio: MCP Tools Specification

## Purpose

pipeio exposes tools through projio's MCP server for AI agent access to pipeline management operations.

## Tool Registration

Tools are registered in `src/projio/mcp/server.py` following the same pattern as codio, biblio, and notio. The implementation lives in two layers:

1. **`src/projio/mcp/pipeio.py`** — thin MCP wrappers with availability checks
2. **`packages/pipeio/src/pipeio/mcp.py`** — actual tool logic

## Tools

### `pipeio_flow_list`

List pipeline flows, optionally filtered by pipe.

```
pipeio_flow_list(pipe: str = "") → dict
```

**Returns:**
```json
{
  "flows": [
    {
      "name": "ieeg",
      "pipe": "preprocess",
      "code_path": "code/pipelines/preprocess/ieeg",
      "config_path": "code/pipelines/preprocess/ieeg/config.yml",
      "doc_path": "docs/explanation/pipelines/pipe-preprocess/flow-ieeg",
      "mods": {"badchannel": {...}, "linenoise": {...}}
    }
  ]
}
```

### `pipeio_flow_status`

Show status of a specific pipeline flow.

```
pipeio_flow_status(pipe: str, flow: str) → dict
```

**Returns:**
```json
{
  "pipe": "preprocess",
  "flow": "ieeg",
  "config_exists": true,
  "output_dir": "derivatives/preprocess",
  "registry_groups": ["raw_zarr", "badlabel", "filter", "interpolate"],
  "notebook_count": 2,
  "notebooks_synced": 1,
  "docs_exists": true
}
```

### `pipeio_nb_status`

Show notebook sync and publication status across all flows.

```
pipeio_nb_status() → dict
```

**Returns:**
```json
{
  "flows": [
    {
      "flow": "preprocess/ieeg",
      "notebooks": [
        {
          "name": "investigate_noise_characterization_demo",
          "synced": false,
          "published": false,
          "py_mtime": "2026-03-20T14:30:00",
          "ipynb_mtime": "2026-03-20T14:28:00"
        }
      ]
    }
  ]
}
```

### `pipeio_registry_validate`

Validate pipeline registry consistency.

```
pipeio_registry_validate() → dict
```

**Returns:**
```json
{
  "valid": false,
  "errors": [
    "Flow DGgamma/DGgamma: config_path is null"
  ],
  "warnings": [
    "Flow DGgamma/DGgamma: slug 'DGgamma' does not pass naming convention",
    "Flow dentatespike/dentatespike: no documentation directory"
  ],
  "stats": {
    "pipes": 8,
    "flows": 12,
    "mods": 31,
    "slug_violations": 2,
    "missing_docs": 4
  }
}
```

## Agent Routing

For agent instructions (CLAUDE.md / `agent_instructions` tool):

| Intent | MCP tool | Do NOT |
|--------|----------|--------|
| List pipelines | `pipeio_flow_list()` | Parse registry YAML manually |
| Check flow status | `pipeio_flow_status(pipe, flow)` | Read config.yml directly |
| Check notebook state | `pipeio_nb_status()` | Compare file timestamps manually |
| Validate registry | `pipeio_registry_validate()` | Run validation scripts directly |

## Graceful Degradation

All tools check for pipeio availability:

```python
def _pipeio_available() -> bool:
    try:
        import pipeio
        return True
    except ImportError:
        return False
```

When pipeio is not installed, tools return:

```json
{"error": "pipeio_flow_list requires the pipeio package. Install with: pip install pipeio"}
```
