# Manage flows

## List all flows

```bash
pipeio flow list
```

Filter by pipe:

```bash
pipeio flow list --pipe preprocess
```

## Create a new flow

```bash
pipeio flow new <pipe> <flow>
```

This creates a scaffold directory with `config.yml` and `Snakefile`:

```bash
pipeio flow new preprocess ecephys
```

```
pipelines/preprocess/ecephys/
  config.yml
  Snakefile
```

If `code/pipelines/` exists, the flow is created there; otherwise under `pipelines/`.

## Inspect a flow via MCP

When using pipeio through projio's MCP server, the `pipeio_flow_status` tool returns config details, output groups, and notebook counts without requiring manual file inspection.
