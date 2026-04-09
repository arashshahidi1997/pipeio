"""CLI entry point for pipeio."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def _find_root(start: Path | None = None) -> Path:
    """Walk up from *start* looking for pipeio project markers."""
    cwd = start or Path.cwd()
    for d in [cwd, *cwd.parents]:
        if (d / ".projio" / "pipeio").is_dir():
            return d
        if (d / ".pipeio").is_dir():
            return d
        if (d / ".projio" / "config.yml").exists():
            return d
        if (d / ".git").exists():
            return d
    return cwd


def _detect_flow_from_cwd(root: Path) -> str | None:
    """Try to detect flow name from cwd by matching against registry entries."""
    from pipeio.registry import PipelineRegistry

    reg_path = _find_registry(root)
    if not reg_path or not reg_path.exists():
        return None

    cwd = Path.cwd().resolve()
    registry = PipelineRegistry.from_yaml(reg_path)
    for entry in registry.list_flows():
        code_path = Path(entry.code_path)
        if not code_path.is_absolute():
            code_path = root / code_path
        code_path = code_path.resolve()
        # cwd is inside this flow's code directory
        if cwd == code_path or code_path in cwd.parents:
            return entry.name
    return None


def _find_registry(root: Path) -> Path | None:
    """Locate the pipeline registry, checking .projio/pipeio/ first."""
    from pipeio.registry import find_registry
    return find_registry(root)


def _pipeio_dir(root: Path) -> Path:
    """Return the pipeio config directory, preferring .projio/pipeio/ if .projio/ exists."""
    projio_dir = root / ".projio" / "pipeio"
    if projio_dir.exists():
        return projio_dir
    if (root / ".projio").is_dir():
        return projio_dir  # will be created under .projio/
    return root / ".pipeio"


def _cmd_init(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else _find_root()
    pipeio_dir = _pipeio_dir(root)

    if pipeio_dir.exists():
        print(f"pipeio already initialized at {pipeio_dir}")
        return 0

    pipeio_dir.mkdir(parents=True)
    reg_path = pipeio_dir / "registry.yml"
    reg_path.write_text(
        "# pipeio pipeline registry\nflows: {}\n",
        encoding="utf-8",
    )

    templates_dir = pipeio_dir / "templates" / "flow"
    templates_dir.mkdir(parents=True)

    print(f"Initialized pipeio at {pipeio_dir}")
    return 0


def _load_registry(root: Path):
    """Load the pipeline registry, returning (registry, root) or exiting with error."""
    from pipeio.registry import PipelineRegistry

    reg_path = _find_registry(root)
    if reg_path is None:
        print("No registry found. Run 'pipeio init' first.", file=sys.stderr)
        return None
    return PipelineRegistry.from_yaml(reg_path)


def _resolve_flow(root: Path, flow_name: str):
    """Look up a flow by name. Returns entry or None."""
    registry = _load_registry(root)
    if registry is None:
        return None

    for f in registry.list_flows():
        if f.name == flow_name:
            return f

    print(f"Unknown flow: {flow_name}", file=sys.stderr)
    return None


def _flow_code_dir(root: Path, entry) -> Path:
    """Return the absolute code directory for a flow entry."""
    code_path = Path(entry.code_path)
    return code_path if code_path.is_absolute() else root / code_path


def _flow_config_path(root: Path, entry) -> Path | None:
    """Return the absolute config path for a flow entry, or None."""
    if not entry.config_path:
        return None
    cfg_path = Path(entry.config_path)
    return cfg_path if cfg_path.is_absolute() else root / cfg_path


def _cmd_flow_list(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else _find_root()
    registry = _load_registry(root)
    if registry is None:
        return 1

    flows = registry.list_flows()
    if not flows:
        print("No flows registered.")
        return 0

    for f in flows:
        config = f"  config={f.config_path}" if f.config_path else ""
        mods = f"  mods={len(f.mods)}" if f.mods else ""
        print(f"  {f.name}  code={f.code_path}{config}{mods}")
    return 0


def _cmd_flow_ids(args: argparse.Namespace) -> int:
    """Print flow names for shell completion."""
    root = Path(args.root) if args.root else _find_root()
    registry = _load_registry(root)
    if registry is None:
        return 1

    names = sorted({f.name for f in registry.list_flows()})
    print(" ".join(names))
    return 0


def _cmd_flow_path(args: argparse.Namespace) -> int:
    """Print absolute code_path for a flow (for shell cd)."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1
    print(_flow_code_dir(root, entry))
    return 0


def _cmd_flow_config(args: argparse.Namespace) -> int:
    """Print absolute config_path for a flow."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1
    cfg_path = _flow_config_path(root, entry)
    if cfg_path is None:
        print(f"No config_path for {args.flow}", file=sys.stderr)
        return 1
    print(cfg_path)
    return 0


def _cmd_flow_deriv(args: argparse.Namespace) -> int:
    """Print absolute derivative directory path for a flow."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    cfg_path = _flow_config_path(root, entry)
    if cfg_path is None:
        print(f"No config_path for {args.flow}", file=sys.stderr)
        return 1
    if not cfg_path.exists():
        print(f"Config not found: {cfg_path}", file=sys.stderr)
        return 1

    config = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    output_dir = config.get("output_dir", "")
    if not output_dir:
        print(f"No output_dir in config for {args.flow}", file=sys.stderr)
        return 1

    deriv_path = Path(output_dir)
    if not deriv_path.is_absolute():
        deriv_path = root / deriv_path
    print(deriv_path)
    return 0


def _cmd_flow_smk(args: argparse.Namespace) -> int:
    """Run snakemake in the context of a flow (resolves --snakefile, --directory)."""
    import subprocess

    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    flow_dir = _flow_code_dir(root, entry)
    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        print(f"No Snakefile in {flow_dir}", file=sys.stderr)
        return 1

    smk_cmd = _resolve_snakemake()
    cmd = [
        *smk_cmd,
        "--snakefile", str(snakefile),
        "--directory", str(flow_dir),
        *args.smk_args,
    ]

    result = subprocess.run(cmd, cwd=str(root))
    return result.returncode


def _cmd_flow_status(args: argparse.Namespace) -> int:
    """Show flow status: config, outputs, completion summary."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    flow_dir = _flow_code_dir(root, entry)
    cfg_path = _flow_config_path(root, entry)

    print(f"  flow:    {entry.name}")
    print(f"  code:    {flow_dir}")
    print(f"  config:  {cfg_path or '(none)'}")

    snakefile = flow_dir / "Snakefile"
    print(f"  Snakefile: {'yes' if snakefile.exists() else 'NO'}")

    if entry.mods:
        print(f"  mods:    {', '.join(entry.mods.keys())}")

    # Show output groups and file counts if config exists
    if cfg_path and cfg_path.exists():
        config = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        output_dir = config.get("output_dir", "")
        registry_data = config.get("registry") or {}

        if output_dir:
            output_base = Path(output_dir)
            if not output_base.is_absolute():
                output_base = root / output_base
            print(f"  output:  {output_base}")

            for group_name, group in registry_data.items():
                if not isinstance(group, dict):
                    continue
                group_root = (group.get("bids") or {}).get("root", group_name)
                group_dir = output_base / group_root
                members = group.get("members") or {}
                if group_dir.exists():
                    total = sum(
                        len(list(group_dir.glob(f"**/*{m.get('suffix', '')}{m.get('extension', '')}")))
                        for m in members.values()
                        if isinstance(m, dict)
                    )
                    print(f"    {group_name}: {len(members)} members, {total} files")
                else:
                    print(f"    {group_name}: {len(members)} members, (no output dir)")
    return 0


def _cmd_flow_targets(args: argparse.Namespace) -> int:
    """Resolve output paths for a flow's registry entries."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    cfg_path = _flow_config_path(root, entry)
    if cfg_path is None or not cfg_path.exists():
        print(f"No config for {args.flow}", file=sys.stderr)
        return 1

    from pipeio.config import FlowConfig
    from pipeio.resolver import PipelineContext

    flow_config = FlowConfig.from_yaml(cfg_path)
    ctx = PipelineContext.from_config(flow_config, root)

    group_filter = getattr(args, "group", None)
    member_filter = getattr(args, "member", None)

    # Parse --entity key=value pairs
    entities = {}
    for kv in getattr(args, "entity", []) or []:
        if "=" in kv:
            k, v = kv.split("=", 1)
            entities[k] = v

    groups = [group_filter] if group_filter else ctx.groups()

    for group_name in groups:
        members = [member_filter] if member_filter else ctx.products(group_name)
        for member_name in members:
            if entities:
                # Resolve or expand
                if getattr(args, "expand", False):
                    paths = ctx.expand(group_name, member_name, **entities)
                    for p in paths:
                        exists = "  [exists]" if p.exists() else ""
                        print(f"  {group_name}/{member_name}: {p}{exists}")
                    if not paths:
                        print(f"  {group_name}/{member_name}: (no matches)")
                else:
                    path = ctx.path(group_name, member_name, **entities)
                    exists = "  [exists]" if path.exists() else ""
                    print(f"  {group_name}/{member_name}: {path}{exists}")
            else:
                # Show pattern
                pattern = ctx.pattern(group_name, member_name)
                print(f"  {group_name}/{member_name}: {pattern}")
    return 0


def _cmd_flow_run(args: argparse.Namespace) -> int:
    """Launch snakemake via screen with optional wildcard filtering."""
    import json
    import shutil

    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    if not shutil.which("screen"):
        print("screen is not installed. Install with: apt install screen", file=sys.stderr)
        return 1

    # Parse --filter key=value pairs into wildcards
    wildcards = {}
    for kv in getattr(args, "filter", []) or []:
        if "=" in kv:
            k, v = kv.split("=", 1)
            wildcards[k] = v

    from pipeio.mcp import mcp_run

    result = mcp_run(
        root,
        pipe=entry.name,
        flow=entry.name,
        targets=getattr(args, "targets", None) or None,
        cores=getattr(args, "cores", 1),
        dryrun=getattr(args, "dryrun", False),
        snakemake_cmd=_resolve_snakemake(),
        wildcards=wildcards or None,
    )

    if "error" in result:
        print(f"Error: {result['error']}", file=sys.stderr)
        return 1

    print(f"  run_id:  {result['run_id']}")
    print(f"  screen:  {result['screen']}")
    print(f"  log:     {result['log_path']}")
    if result.get("dryrun"):
        print("  (dry run)")
    return 0


def _cmd_flow_log(args: argparse.Namespace) -> int:
    """Tail the latest run log for a flow."""
    import json

    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    from pipeio.mcp import _load_runs

    runs = _load_runs(root)
    # Filter to this flow, most recent first
    flow_runs = [
        r for r in runs
        if r.get("flow") == entry.name
    ]
    if not flow_runs:
        print(f"No runs recorded for {entry.name}", file=sys.stderr)
        return 1

    latest = flow_runs[-1]
    log_path = root / latest.get("log_path", "")

    if not log_path.exists():
        print(f"Log not found: {log_path}", file=sys.stderr)
        return 1

    lines = int(getattr(args, "lines", 40) or 40)
    text = log_path.read_text(encoding="utf-8", errors="replace")
    tail = text.splitlines()[-lines:]

    print(f"  run_id: {latest['id']}  status: {latest.get('status', '?')}  log: {log_path}")
    print("  " + "-" * 60)
    for line in tail:
        print(line)
    return 0


def _cmd_flow_mods(args: argparse.Namespace) -> int:
    """List mods for a flow with their rules."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    if not entry.mods:
        print(f"No mods defined for {entry.name}")
        return 0

    for mod_name, mod in entry.mods.items():
        rules = ", ".join(mod.rules) if mod.rules else "(none)"
        desc = f"  # {mod.description}" if getattr(mod, "description", None) else ""
        print(f"  {mod_name}: {rules}{desc}")
    return 0


def _cmd_flow_dag(args: argparse.Namespace) -> int:
    """Generate DAG SVG for a flow."""
    import shutil
    import subprocess

    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    flow_dir = _flow_code_dir(root, entry)
    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        print(f"No Snakefile in {flow_dir}", file=sys.stderr)
        return 1

    fmt = getattr(args, "format", "svg") or "svg"
    smk_cmd = _resolve_snakemake()

    # Build snakemake command
    graph_flag = "--dag" if getattr(args, "full", False) else "--rulegraph"
    cmd = [*smk_cmd, "--snakefile", str(snakefile), "--directory", str(flow_dir), graph_flag]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(root), timeout=60, check=False)
    if result.returncode != 0:
        print(f"Snakemake failed: {result.stderr[:500]}", file=sys.stderr)
        return 1

    dot_output = result.stdout

    if fmt == "dot":
        print(dot_output)
        return 0

    # Convert to SVG
    if not shutil.which("dot"):
        print("graphviz 'dot' not found — install with: apt install graphviz", file=sys.stderr)
        print(dot_output)
        return 1

    svg_result = subprocess.run(
        ["dot", "-Tsvg"], input=dot_output, capture_output=True, text=True, timeout=30, check=False,
    )
    if svg_result.returncode != 0:
        print(f"graphviz dot failed: {svg_result.stderr[:500]}", file=sys.stderr)
        return 1

    # Write to docs path
    out = root / "docs" / "pipelines" / entry.name / "dag.svg"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg_result.stdout, encoding="utf-8")
    print(f"  written: {out.relative_to(root)}")
    return 0


def _cmd_flow_report(args: argparse.Namespace) -> int:
    """Generate snakemake HTML report for a flow."""
    root = Path(args.root) if args.root else _find_root()
    entry = _resolve_flow(root, args.flow)
    if entry is None:
        return 1

    from pipeio.mcp import mcp_report

    result = mcp_report(
        root,
        flow=entry.name,
        target=getattr(args, "target", "") or "",
        snakemake_cmd=_resolve_snakemake(),
    )

    if "error" in result:
        print(f"Error: {result['error']}", file=sys.stderr)
        return 1

    print(f"  report:  {result['report_path']}")
    print(f"  size:    {result.get('size_kb', 0)} KB")
    print(f"  targets: {result.get('targets_resolved', 0)} outputs resolved")
    return 0


def _resolve_snakemake() -> list[str]:
    """Find snakemake binary, with conda env wrapping if needed."""
    import re
    import shutil

    binary = shutil.which("snakemake")
    if binary:
        # Check if it's inside a conda env and wrap accordingly
        m = re.search(r"/envs/([^/]+)/bin/([^/]+)$", binary)
        if m:
            env_name, cmd_name = m.group(1), m.group(2)
            envs_dir = binary[: m.start() + len("/envs/") - len("/envs/")]
            for candidate in ("condabin/conda", "bin/conda"):
                conda_bin = f"{envs_dir}/{candidate}"
                if Path(conda_bin).is_file():
                    return [conda_bin, "run", "-n", env_name, cmd_name]
        return [binary]

    # Fallback: try common conda env names
    for env in ("cogpy", "snakemake"):
        for base in ("/storage/share/python/environments/Anaconda3",):
            for rel in ("condabin/conda", "bin/conda"):
                conda = Path(base) / rel
                if conda.is_file():
                    return [str(conda), "run", "-n", env, "snakemake"]

    # Last resort
    print("warning: snakemake not found, trying bare 'snakemake'", file=sys.stderr)
    return ["snakemake"]


def _cmd_flow_new(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else _find_root()
    pipelines_dir = root / "code" / "pipelines" if (root / "code" / "pipelines").exists() else root / "pipelines"

    flow = args.flow
    flow_dir = pipelines_dir / flow
    is_new = not flow_dir.exists()

    if flow_dir.exists() and (flow_dir / "Snakefile").exists():
        print(f"Flow already exists: {flow_dir}", file=sys.stderr)
        print(f"  Augmenting missing directories...", file=sys.stderr)

    # Create directory structure (idempotent — fills in missing dirs)
    flow_dir.mkdir(parents=True, exist_ok=True)
    created: list[str] = []

    for d in [
        "scripts",
        "rules",
        "docs",
        "notebooks/explore/.src",
        "notebooks/explore/.myst",
        "notebooks/demo/.src",
        "notebooks/demo/.myst",
    ]:
        path = flow_dir / d
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            created.append(d)

    # config.yml (only if new)
    cfg_path = flow_dir / "config.yml"
    if not cfg_path.exists():
        cfg_path.write_text(
            f"# config for {flow}\n"
            f"input_dir: \"\"\n"
            f"output_dir: \"derivatives/{flow}\"\n"
            f"input_manifest: \"\"\n"
            f"output_manifest: \"derivatives/{flow}/manifest.yml\"\n"
            f"registry: {{}}\n",
            encoding="utf-8",
        )
        created.append("config.yml")

    # Snakefile (only if new)
    snakefile = flow_dir / "Snakefile"
    if not snakefile.exists():
        snakefile.write_text(
            f"# Snakefile for {flow}\n"
            f"from pathlib import Path\n"
            f"\n"
            f"configfile: \"config.yml\"\n"
            f"\n"
            f"\n"
            f"rule all:\n"
            f"    input: []\n",
            encoding="utf-8",
        )
        created.append("Snakefile")

    # publish.yml (only if new)
    pub_path = flow_dir / "publish.yml"
    if not pub_path.exists():
        pub_path.write_text(
            f"dag: true\n"
            f"report: false\n"
            f"scripts: true\n",
            encoding="utf-8",
        )
        created.append("publish.yml")

    # Makefile (only if new)
    makefile = flow_dir / "Makefile"
    if not makefile.exists():
        makefile.write_text(
            f"# Flow: {flow}\n"
            f"SHELL := /bin/bash\n"
            f"\n"
            f"# Project root (relative to this Makefile)\n"
            f"PROJECT_ROOT ?= $(shell git -C $(dir $(abspath $(lastword $(MAKEFILE_LIST)))) rev-parse --show-toplevel)\n"
            f"\n"
            f".PHONY: help run dry-run nb-status nb-sync nb-lab nb-publish\n"
            f"\n"
            f"help:\n"
            f"\t@echo \"Targets: run dry-run nb-status nb-sync nb-lab nb-publish\"\n"
            f"\n"
            f"run:\n"
            f"\tsnakemake --snakefile $(CURDIR)/Snakefile --directory $(PROJECT_ROOT) -j1\n"
            f"\n"
            f"dry-run:\n"
            f"\tsnakemake --snakefile $(CURDIR)/Snakefile --directory $(PROJECT_ROOT) -j1 -n\n"
            f"\n"
            f"nb-status:\n"
            f"\tpipeio nb status\n"
            f"\n"
            f"nb-sync:\n"
            f"\tpipeio nb sync --direction py2nb\n"
            f"\n"
            f"nb-lab:\n"
            f"\tpipeio nb lab\n"
            f"\n"
            f"nb-publish:\n"
            f"\tpipeio nb sync --direction py2nb --force\n"
            f"\tpipeio nb publish\n",
            encoding="utf-8",
        )
        created.append("Makefile")

    # notebook.yml (only if new)
    nb_cfg_path = flow_dir / "notebooks" / "notebook.yml"
    if not nb_cfg_path.exists():
        nb_name = f"explore_{flow}"
        nb_path = flow_dir / "notebooks" / "explore" / ".src" / f"{nb_name}.py"
        nb_path.write_text(
            f"# ---\n"
            f"# jupyter:\n"
            f"#   jupytext:\n"
            f"#     text_representation:\n"
            f"#       format_name: percent\n"
            f"# ---\n"
            f"\n"
            f"# %% [markdown]\n"
            f"# # Explore {flow.replace('_', ' ').title()}\n"
            f"#\n"
            f"# Initial exploration notebook for {flow}.\n"
            f"\n"
            f"# %% [markdown]\n"
            f"# ## Setup\n"
            f"\n"
            f"# %%\n"
            f"from pathlib import Path\n"
            f"\n"
            f"# %% [markdown]\n"
            f"# ## Analysis\n"
            f"\n"
            f"# %%\n",
            encoding="utf-8",
        )

        import yaml
        nb_cfg = {
            "kernel": "",
            "publish": {"docs_dir": "", "prefix": "nb-"},
            "entries": [{
                "path": f"notebooks/explore/.src/{nb_name}.py",
                "kind": "explore",
                "description": f"Initial exploration for {flow}",
                "status": "draft",
                "pair_ipynb": True,
                "pair_myst": True,
            }],
        }
        nb_cfg_path.write_text(
            yaml.safe_dump(nb_cfg, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
        created.append("notebook.yml")
        created.append(f"notebooks/explore/.src/{nb_name}.py")

    # docs/index.md (only if new)
    docs_index = flow_dir / "docs" / "index.md"
    if not docs_index.exists():
        docs_index.write_text(
            f"# {flow}\n"
            f"\n"
            f"Flow: `{flow}`\n"
            f"\n"
            f"## Mods\n"
            f"\n"
            f"(none yet)\n",
            encoding="utf-8",
        )
        created.append("docs/index.md")

    if is_new:
        print(f"Created flow scaffold: {flow_dir}")
    else:
        print(f"Augmented flow: {flow_dir}")
    if created:
        for c in created:
            print(f"  + {c}")
    else:
        print("  (nothing missing)")
    return 0


def _cmd_flow_fork(args: argparse.Namespace) -> int:
    from pipeio.mcp import mcp_flow_fork

    root = Path(args.root) if args.root else _find_root()
    result = mcp_flow_fork(
        root, flow=args.flow, new_flow=args.new_flow,
    )
    if "error" in result:
        print(result["error"], file=sys.stderr)
        return 1

    print(f"Forked: {result['source']} → {result['target']}")
    print(f"  code_path: {result['code_path']}")
    if result.get("mods"):
        print(f"  mods: {', '.join(result['mods'])}")
    return 0


def _cmd_registry_scan(args: argparse.Namespace) -> int:
    from pipeio.registry import PipelineRegistry

    root = Path(args.root) if args.root else _find_root()

    # Determine pipelines directory
    if hasattr(args, "pipelines_dir") and args.pipelines_dir:
        pipelines_dir = Path(args.pipelines_dir)
    elif (root / "code" / "pipelines").exists():
        pipelines_dir = root / "code" / "pipelines"
    elif (root / "pipelines").exists():
        pipelines_dir = root / "pipelines"
    else:
        print("No pipelines directory found.", file=sys.stderr)
        return 1

    docs_dir = None
    if (root / "docs" / "explanation" / "pipelines").exists():
        docs_dir = root / "docs" / "explanation" / "pipelines"

    # Load ignore list
    ignore: set[str] = set()
    ignore_path = _pipeio_dir(root) / "registry_ignore.yml"
    if ignore_path.exists():
        import yaml
        raw = yaml.safe_load(ignore_path.read_text(encoding="utf-8")) or {}
        ignore = set(raw.get("ignore", []))
        if ignore:
            print(f"Ignoring {len(ignore)} deregistered flow(s): {', '.join(sorted(ignore))}")

    registry = PipelineRegistry.scan(pipelines_dir, docs_dir=docs_dir, ignore=ignore)

    # Print summary
    flows = registry.list_flows()
    print(f"Scanned {pipelines_dir}")
    for f in flows:
        config = "config=yes" if f.config_path else "config=no"
        mods = f"mods={len(f.mods)}" if f.mods else ""
        docs = "docs=yes" if f.doc_path else ""
        parts = [f"  flow={f.name}", config]
        if mods:
            parts.append(mods)
        if docs:
            parts.append(docs)
        print("  ".join(parts))

    # Write registry
    if hasattr(args, "output") and args.output:
        output = Path(args.output)
    else:
        output = _pipeio_dir(root) / "registry.yml"
    registry.to_yaml(output)
    print(f"Written: {output} ({len(flows)} flows)")
    return 0


def _cmd_nb_scan(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_scan

    root = Path(args.root) if args.root else _find_root()
    register = getattr(args, "register", False)

    results = nb_scan(root, register=register)
    if not results:
        print("No percent-format notebooks found in notebooks/ directories.")
        return 0

    unregistered = [r for r in results if not r["registered"]]
    registered = [r for r in results if r["registered"]]

    if registered:
        print(f"Registered ({len(registered)}):")
        for r in registered:
            print(f"  {r['name']}  ({r['rel_path']})")

    if unregistered:
        print(f"\nUnregistered ({len(unregistered)}):")
        for r in unregistered:
            tag = " → registered" if r.get("newly_registered") else ""
            print(f"  {r['name']}  ({r['rel_path']}){tag}")
        if not register:
            print("\n  Run with --register to auto-add to notebook.yml")

    return 0


def _cmd_nb_migrate(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_migrate

    root = Path(args.root) if args.root else _find_root()
    execute = getattr(args, "yes", False)

    actions = nb_migrate(root, dry_run=not execute)
    if not actions:
        print("All notebooks already use .src/ layout (nothing to migrate).")
        return 0

    mode = "Migrated" if execute else "Would migrate"
    print(f"{mode} {len(actions)} notebook(s):")
    for a in actions:
        print(f"\n  {a['name']} ({a['flow_root']})")
        for m in a["moves"]:
            print(f"    {m['from']}")
            print(f"    → {m['to']}")
        if "path_update" in a:
            print(f"    notebook.yml: {a['path_update']['from']} → {a['path_update']['to']}")

    if not execute:
        print("\nDry run — pass --yes to execute.")
    return 0


def _cmd_nb_status(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_status

    root = Path(args.root) if args.root else _find_root()
    statuses = nb_status(root)
    if not statuses:
        print("No notebooks found (no notebooks/notebook.yml in project).")
        return 0

    for s in statuses:
        synced = "yes" if s["synced"] else "no"
        executed = "yes" if s["executed"] else "no"
        ipynb = f"  ipynb={'yes' if s['ipynb_exists'] else 'no'}" if s["ipynb_exists"] is not None else ""
        myst = f"  myst={'yes' if s['myst_exists'] else 'no'}" if s["myst_exists"] is not None else ""
        print(f"  {s['name']:<20} py={'yes' if s['py_exists'] else 'no'}{ipynb}{myst}  synced={synced}  executed={executed}")
    return 0


def _cmd_nb_pair(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_pair

    root = Path(args.root) if args.root else _find_root()
    try:
        created = nb_pair(root, force=getattr(args, "force", False))
    except ImportError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if created:
        for p in created:
            print(f"  paired: {p}")
    else:
        print("No notebooks to pair (all up-to-date or no entries).")
    return 0


def _cmd_nb_sync(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else _find_root()
    direction = getattr(args, "direction", "py2nb")
    force = getattr(args, "force", False)

    if direction == "nb2py" or force:
        from pipeio.notebook.lifecycle import find_notebook_configs, nb_sync_one
        try:
            for flow_root, cfg in find_notebook_configs(root):
                for entry in cfg.entries:
                    py_path = flow_root / entry.path
                    result = nb_sync_one(py_path, direction=direction, force=force)
                    if result.get("synced"):
                        for p in result.get("generated", result.get("updated", [])):
                            print(f"  synced: {p}")
                    elif result.get("error"):
                        print(f"  skip: {py_path.stem}: {result['error']}")
        except ImportError as exc:
            print(str(exc), file=sys.stderr)
            return 1
    else:
        from pipeio.notebook.lifecycle import nb_sync
        try:
            updated = nb_sync(root)
        except ImportError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if updated:
            for p in updated:
                print(f"  synced: {p}")
        else:
            print("All notebooks are up-to-date.")
    return 0


def _cmd_nb_diff(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import find_notebook_configs, nb_diff

    root = Path(args.root) if args.root else _find_root()
    found = False
    for flow_root, cfg in find_notebook_configs(root):
        for entry in cfg.entries:
            py_path = flow_root / entry.path
            result = nb_diff(py_path)
            status = result.get("status", "unknown")
            rec = result.get("recommendation", "")
            print(f"  {py_path.stem}: {status} — {rec}")
            found = True
    if not found:
        print("No notebooks found.")
    return 0


def _cmd_nb_lab(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_lab

    root = Path(args.root) if args.root else _find_root()
    flow = getattr(args, "flow", None)
    do_sync = getattr(args, "sync", False)
    refresh_only = getattr(args, "refresh", False)

    # Auto-detect flow from cwd if not specified
    if not flow:
        detected = _detect_flow_from_cwd(root)
        if detected:
            flow = detected
            print(f"Detected flow: {flow}")

    try:
        result = nb_lab(root, flow=flow, sync=do_sync)
    except ImportError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    count = result.get("count", 0)
    lab_dir = result.get("lab_dir", "")
    print(f"Lab manifest: {lab_dir}  ({count} notebook(s) linked)")
    for item in result.get("linked", []):
        print(f"  {item.get('flow', item.get('name', ''))}/{item['name']}")
    for s in result.get("stale_cleaned", []):
        print(f"  removed stale: {s}")

    if refresh_only or count == 0:
        return 0

    # Launch Jupyter Lab
    import subprocess
    print(f"\nStarting Jupyter Lab in {lab_dir} ...")
    subprocess.run(["jupyter", "lab"], cwd=lab_dir)
    return 0


def _cmd_nb_exec(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_exec

    root = Path(args.root) if args.root else _find_root()
    try:
        executed = nb_exec(root)
    except ImportError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if executed:
        for p in executed:
            print(f"  executed: {p}")
    else:
        print("No notebooks executed (no paired .ipynb files found).")
    return 0


def _cmd_nb_publish(args: argparse.Namespace) -> int:
    from pipeio.notebook.lifecycle import nb_publish

    root = Path(args.root) if args.root else _find_root()
    try:
        published = nb_publish(root)
    except ImportError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if published:
        for p in published:
            print(f"  published: {p}")
    else:
        print("Nothing published (no publish_html/publish_myst entries or docs_dir not set).")
    return 0


def _cmd_docs_collect(args: argparse.Namespace) -> int:
    from pipeio.docs import docs_collect

    root = Path(args.root) if args.root else _find_root()
    export = not getattr(args, "no_export", False)
    try:
        collected = docs_collect(root, export=export)
    except ImportError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if collected:
        for p in collected:
            print(f"  collected: {p}")
        print(f"{len(collected)} file(s) collected into docs/pipelines/")
    else:
        print("Nothing to collect (no registry, no flow docs, or no publishable notebooks).")
    return 0


def _cmd_docs_nav(args: argparse.Namespace) -> int:
    from pipeio.docs import docs_nav

    root = Path(args.root) if args.root else _find_root()
    fragment = docs_nav(root)
    print(fragment)
    return 0


def _cmd_contracts_validate(args: argparse.Namespace) -> int:
    from pipeio.contracts import validate_flow_contracts

    root = Path(args.root) if args.root else _find_root()
    results = validate_flow_contracts(root)

    if not results:
        print("No registry found or no flows to validate.")
        return 0

    has_errors = False
    for fv in results:
        status = "OK" if fv.ok else "FAIL"
        print(f"  [{status}] {fv.flow_id}")
        for p in fv.passed:
            print(f"    PASS: {p}")
        for w in fv.warnings:
            print(f"    WARN: {w}")
        for e in fv.errors:
            print(f"    ERROR: {e}")
        if not fv.ok:
            has_errors = True

    return 2 if has_errors else 0


def _cmd_registry_validate(args: argparse.Namespace) -> int:
    from pipeio.registry import PipelineRegistry

    root = Path(args.root) if args.root else _find_root()
    if hasattr(args, "registry") and args.registry:
        reg_path = Path(args.registry)
    else:
        reg_path = _find_registry(root)

    if reg_path is None or not reg_path.exists():
        print("No registry found. Run 'pipeio init' first.", file=sys.stderr)
        return 1

    registry = PipelineRegistry.from_yaml(reg_path)
    result = registry.validate(root=root)

    if result.warnings:
        for w in result.warnings:
            print(f"  WARN: {w}")
    if result.errors:
        for e in result.errors:
            print(f"  ERROR: {e}")
        return 2

    if not result.warnings:
        print("Registry is valid.")
    return 0


def _cmd_registry_deregister(args: argparse.Namespace) -> int:
    from pipeio.registry import PipelineRegistry

    root = Path(args.root) if args.root else _find_root()
    reg_path = _find_registry(root)
    if reg_path is None or not reg_path.exists():
        print("No registry found.", file=sys.stderr)
        return 1

    registry = PipelineRegistry.from_yaml(reg_path)
    try:
        removed = registry.remove(args.flow)
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    registry.to_yaml(reg_path)
    print(f"Deregistered: flow={removed.name}")
    print(f"  code_path: {removed.code_path}")
    if removed.mods:
        print(f"  mods: {', '.join(removed.mods.keys())}")
    print("  Note: files on disk are untouched.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pipeio",
        description="Pipeline registry, notebook lifecycle, and flow management",
    )
    parser.add_argument("--root", help="Project root (auto-detected if omitted)")
    sub = parser.add_subparsers(dest="command")

    # pipeio init
    init_p = sub.add_parser("init", help="Scaffold pipeio config in the current project")
    init_p.add_argument("--root", dest="root", help="Project root")

    # pipeio flow {list,new,status}
    flow_p = sub.add_parser("flow", help="Flow management")
    flow_p.add_argument("--root", dest="root", help="Project root")
    flow_sub = flow_p.add_subparsers(dest="flow_command")

    flow_list = flow_sub.add_parser("list", help="List all flows")

    flow_new = flow_sub.add_parser("new", help="Scaffold a new flow")
    flow_new.add_argument("flow", help="Flow name")

    flow_fork_p = flow_sub.add_parser("fork", help="Fork (copy) an existing flow")
    flow_fork_p.add_argument("flow", help="Source flow name")
    flow_fork_p.add_argument("new_flow", help="Name for the forked flow")

    flow_ids = flow_sub.add_parser("ids", help="Print flow names (for shell completion)")

    flow_path = flow_sub.add_parser("path", help="Print code directory path for a flow")
    flow_path.add_argument("flow", help="Flow name")

    flow_config = flow_sub.add_parser("config", help="Print config path for a flow")
    flow_config.add_argument("flow", help="Flow name")

    flow_deriv = flow_sub.add_parser("deriv", help="Print derivative directory path for a flow")
    flow_deriv.add_argument("flow", help="Flow name")

    flow_smk = flow_sub.add_parser("smk", help="Run snakemake in a flow's context")
    flow_smk.add_argument("flow", help="Flow name")
    flow_smk.add_argument("smk_args", nargs=argparse.REMAINDER, help="Snakemake arguments")

    flow_status = flow_sub.add_parser("status", help="Show flow status and output summary")
    flow_status.add_argument("flow", help="Flow name")

    flow_targets = flow_sub.add_parser("targets", help="Resolve output paths for a flow")
    flow_targets.add_argument("flow", help="Flow name")
    flow_targets.add_argument("--group", "-g", help="Registry group name")
    flow_targets.add_argument("--member", "-m", help="Registry member name")
    flow_targets.add_argument("--entity", "-e", action="append", help="Entity filter (key=value, repeatable)")
    flow_targets.add_argument("--expand", "-x", action="store_true", help="Glob for all matching paths")

    flow_run = flow_sub.add_parser("run", help="Launch snakemake in a screen session")
    flow_run.add_argument("flow", help="Flow name")
    flow_run.add_argument("targets", nargs="*", help="Snakemake target rules")
    flow_run.add_argument("--cores", "-c", type=int, default=1, help="Number of cores")
    flow_run.add_argument("--dryrun", "-n", action="store_true", help="Dry run")
    flow_run.add_argument("--filter", "-f", action="append", help="Wildcard filter (key=value, repeatable)")

    flow_log = flow_sub.add_parser("log", help="Tail the latest run log for a flow")
    flow_log.add_argument("flow", help="Flow name")
    flow_log.add_argument("--lines", "-n", type=int, default=40, help="Number of lines to show")

    flow_mods_p = flow_sub.add_parser("mods", help="List mods for a flow")
    flow_mods_p.add_argument("flow", help="Flow name")

    flow_dag = flow_sub.add_parser("dag", help="Generate DAG SVG for a flow")
    flow_dag.add_argument("flow", help="Flow name")
    flow_dag.add_argument("--format", choices=["svg", "dot"], default="svg", help="Output format (default svg)")
    flow_dag.add_argument("--full", action="store_true", help="Full job DAG instead of rulegraph")

    flow_report = flow_sub.add_parser("report", help="Generate snakemake HTML report")
    flow_report.add_argument("flow", help="Flow name")
    flow_report.add_argument("--target", help="Explicit target rule (overrides auto-resolution)")

    # pipeio nb {pair,sync,exec,publish,status}
    nb_p = sub.add_parser("nb", help="Notebook lifecycle")
    nb_p.add_argument("--root", dest="root", help="Project root")
    nb_sub = nb_p.add_subparsers(dest="nb_command")

    nb_pair_p = nb_sub.add_parser("pair", help="Pair .py notebooks with ipynb/myst")
    nb_pair_p.add_argument("--force", action="store_true", help="Re-create existing pairs")

    nb_sync_p = nb_sub.add_parser("sync", help="Sync notebook formats")
    nb_sync_p.add_argument(
        "--direction", choices=["py2nb", "nb2py"], default="py2nb",
        help="Sync direction: py2nb (default) or nb2py (human edits → .py)",
    )
    nb_sync_p.add_argument("--force", action="store_true", help="Sync even if up-to-date")

    nb_sub.add_parser("diff", help="Show sync state between .py and .ipynb")
    nb_sub.add_parser("exec", help="Execute notebooks")
    nb_sub.add_parser("publish", help="Publish notebooks to docs")
    nb_sub.add_parser("status", help="Show notebook sync status")

    nb_scan_p = nb_sub.add_parser("scan", help="Scan for unregistered notebooks")
    nb_scan_p.add_argument("--register", action="store_true", help="Auto-register found notebooks into notebook.yml")

    nb_migrate_p = nb_sub.add_parser("migrate", help="Migrate notebooks to .src/.myst layout")
    nb_migrate_p.add_argument("--yes", action="store_true", help="Execute migration (default: dry run)")

    nb_lab_p = nb_sub.add_parser("lab", help="Build symlink manifest and launch Jupyter Lab")
    nb_lab_p.add_argument("--flow", help="Filter to a specific flow")
    nb_lab_p.add_argument("--sync", action="store_true", help="Sync py→ipynb before linking")
    nb_lab_p.add_argument("--refresh", action="store_true", help="Refresh manifest only (no Jupyter launch)")

    # pipeio registry {scan,validate}
    reg_p = sub.add_parser("registry", help="Pipeline registry")
    reg_p.add_argument("--root", dest="root", help="Project root")
    reg_sub = reg_p.add_subparsers(dest="registry_command")

    reg_scan = reg_sub.add_parser("scan", help="Scan filesystem for flows")
    reg_scan.add_argument("--pipelines-dir", help="Pipelines directory path")
    reg_scan.add_argument("--output", help="Output YAML path")

    reg_val = reg_sub.add_parser("validate", help="Validate registry consistency")
    reg_val.add_argument("--registry", help="Registry YAML path")

    reg_dereg = reg_sub.add_parser("deregister", help="Remove a flow from the registry")
    reg_dereg.add_argument("flow", help="Flow name")

    # pipeio docs {collect,nav}
    docs_p = sub.add_parser("docs", help="Pipeline documentation")
    docs_p.add_argument("--root", dest="root", help="Project root")
    docs_sub = docs_p.add_subparsers(dest="docs_command")
    docs_collect_p = docs_sub.add_parser("collect", help="Collect flow docs and notebook outputs into docs/pipelines/")
    docs_collect_p.add_argument("--no-export", action="store_true", help="Skip export phase (DAG/notebook generation); collect pre-built artifacts only")
    docs_sub.add_parser("nav", help="Generate MkDocs nav fragment for pipeline docs")

    # pipeio contracts {validate}
    con_p = sub.add_parser("contracts", help="Pipeline contracts")
    con_p.add_argument("--root", dest="root", help="Project root")
    con_sub = con_p.add_subparsers(dest="contracts_command")
    con_sub.add_parser("validate", help="Validate pipeline inputs/outputs")

    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    # Dispatch
    if args.command == "init":
        return _cmd_init(args)

    if args.command == "flow":
        if args.flow_command == "list":
            return _cmd_flow_list(args)
        if args.flow_command == "new":
            return _cmd_flow_new(args)
        if args.flow_command == "fork":
            return _cmd_flow_fork(args)
        if args.flow_command == "ids":
            return _cmd_flow_ids(args)
        if args.flow_command == "path":
            return _cmd_flow_path(args)
        if args.flow_command == "config":
            return _cmd_flow_config(args)
        if args.flow_command == "deriv":
            return _cmd_flow_deriv(args)
        if args.flow_command == "smk":
            return _cmd_flow_smk(args)
        if args.flow_command == "status":
            return _cmd_flow_status(args)
        if args.flow_command == "targets":
            return _cmd_flow_targets(args)
        if args.flow_command == "run":
            return _cmd_flow_run(args)
        if args.flow_command == "log":
            return _cmd_flow_log(args)
        if args.flow_command == "mods":
            return _cmd_flow_mods(args)
        if args.flow_command == "dag":
            return _cmd_flow_dag(args)
        if args.flow_command == "report":
            return _cmd_flow_report(args)
        flow_p.print_help()
        return 0

    if args.command == "registry":
        if args.registry_command == "scan":
            return _cmd_registry_scan(args)
        if args.registry_command == "validate":
            return _cmd_registry_validate(args)
        if args.registry_command == "deregister":
            return _cmd_registry_deregister(args)
        reg_p.print_help()
        return 0

    if args.command == "nb":
        if args.nb_command == "status":
            return _cmd_nb_status(args)
        if args.nb_command == "pair":
            return _cmd_nb_pair(args)
        if args.nb_command == "sync":
            return _cmd_nb_sync(args)
        if args.nb_command == "diff":
            return _cmd_nb_diff(args)
        if args.nb_command == "exec":
            return _cmd_nb_exec(args)
        if args.nb_command == "publish":
            return _cmd_nb_publish(args)
        if args.nb_command == "lab":
            return _cmd_nb_lab(args)
        if args.nb_command == "scan":
            return _cmd_nb_scan(args)
        if args.nb_command == "migrate":
            return _cmd_nb_migrate(args)
        nb_p.print_help()
        return 0

    if args.command == "docs":
        if args.docs_command == "collect":
            return _cmd_docs_collect(args)
        if args.docs_command == "nav":
            return _cmd_docs_nav(args)
        docs_p.print_help()
        return 0

    if args.command == "contracts":
        if args.contracts_command == "validate":
            return _cmd_contracts_validate(args)
        con_p.print_help()
        return 0

    parser.print_help()
    return 0
