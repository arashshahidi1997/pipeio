"""CLI entry point for pipeio."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def _find_root(start: Path | None = None) -> Path:
    """Walk up from *start* looking for .pipeio/, .projio/, or .git."""
    cwd = start or Path.cwd()
    for d in [cwd, *cwd.parents]:
        if (d / ".pipeio").is_dir():
            return d
        if (d / ".projio" / "config.yml").exists():
            return d
        if (d / ".git").exists():
            return d
    return cwd


def _cmd_init(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else _find_root()
    pipeio_dir = root / ".pipeio"

    if pipeio_dir.exists():
        print(f".pipeio/ already exists at {pipeio_dir}")
        return 0

    pipeio_dir.mkdir(parents=True)
    reg_path = pipeio_dir / "registry.yml"
    reg_path.write_text(
        "# pipeio pipeline registry\nflows: {}\n",
        encoding="utf-8",
    )

    templates_dir = pipeio_dir / "templates" / "flow"
    templates_dir.mkdir(parents=True)

    print(f"Initialized .pipeio/ at {pipeio_dir}")
    return 0


def _cmd_flow_list(args: argparse.Namespace) -> int:
    from pipeio.registry import PipelineRegistry

    root = Path(args.root) if args.root else _find_root()
    reg_path = root / ".pipeio" / "registry.yml"
    if not reg_path.exists():
        print(f"No registry found at {reg_path}. Run 'pipeio init' first.", file=sys.stderr)
        return 1

    registry = PipelineRegistry.from_yaml(reg_path)
    pipe = getattr(args, "pipe", None)
    flows = registry.list_flows(pipe=pipe)
    if not flows:
        print("No flows registered.")
        return 0

    for f in flows:
        config = f"  config={f.config_path}" if f.config_path else ""
        mods = f"  mods={len(f.mods)}" if f.mods else ""
        print(f"  {f.pipe}/{f.name}  code={f.code_path}{config}{mods}")
    return 0


def _cmd_flow_new(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else _find_root()
    pipelines_dir = root / "code" / "pipelines" if (root / "code" / "pipelines").exists() else root / "pipelines"

    flow_dir = pipelines_dir / args.pipe / args.flow
    if flow_dir.exists():
        print(f"Flow directory already exists: {flow_dir}", file=sys.stderr)
        return 1

    flow_dir.mkdir(parents=True)
    (flow_dir / "config.yml").write_text(
        f"# config for {args.pipe}/{args.flow}\n"
        f"input_dir: \"\"\n"
        f"output_dir: \"\"\n"
        f"registry: {{}}\n",
        encoding="utf-8",
    )
    (flow_dir / "Snakefile").write_text(
        f"# Snakefile for {args.pipe}/{args.flow}\n",
        encoding="utf-8",
    )

    print(f"Created flow scaffold: {flow_dir}")
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

    registry = PipelineRegistry.scan(pipelines_dir, docs_dir=docs_dir)

    # Print summary
    pipes = registry.list_pipes()
    flows = registry.list_flows()
    print(f"Scanned {pipelines_dir}")
    for f in flows:
        config = "config=yes" if f.config_path else "config=no"
        mods = f"mods={len(f.mods)}" if f.mods else ""
        docs = "docs=yes" if f.doc_path else ""
        parts = [f"  pipe={f.pipe}", f"flow={f.name}", config]
        if mods:
            parts.append(mods)
        if docs:
            parts.append(docs)
        print("  ".join(parts))

    # Write registry
    output = Path(args.output) if hasattr(args, "output") and args.output else root / ".pipeio" / "registry.yml"
    registry.to_yaml(output)
    print(f"Written: {output} ({len(pipes)} pipes, {len(flows)} flows)")
    return 0


def _cmd_registry_validate(args: argparse.Namespace) -> int:
    from pipeio.registry import PipelineRegistry

    root = Path(args.root) if args.root else _find_root()
    reg_path = Path(args.registry) if hasattr(args, "registry") and args.registry else root / ".pipeio" / "registry.yml"

    if not reg_path.exists():
        print(f"No registry found at {reg_path}", file=sys.stderr)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pipeio",
        description="Pipeline registry, notebook lifecycle, and flow management",
    )
    parser.add_argument("--root", help="Project root (auto-detected if omitted)")
    sub = parser.add_subparsers(dest="command")

    # pipeio init
    init_p = sub.add_parser("init", help="Scaffold .pipeio/ in the current project")
    init_p.add_argument("--root", dest="root", help="Project root")

    # pipeio flow {list,new,status}
    flow_p = sub.add_parser("flow", help="Flow management")
    flow_p.add_argument("--root", dest="root", help="Project root")
    flow_sub = flow_p.add_subparsers(dest="flow_command")

    flow_list = flow_sub.add_parser("list", help="List all flows")
    flow_list.add_argument("--pipe", help="Filter by pipe name")

    flow_new = flow_sub.add_parser("new", help="Scaffold a new flow")
    flow_new.add_argument("pipe", help="Pipeline name")
    flow_new.add_argument("flow", help="Flow name")

    # pipeio nb {pair,sync,exec,publish,status}
    nb_p = sub.add_parser("nb", help="Notebook lifecycle")
    nb_sub = nb_p.add_subparsers(dest="nb_command")
    nb_sub.add_parser("pair", help="Pair .py notebooks with ipynb/myst")
    nb_sub.add_parser("sync", help="Sync notebook formats")
    nb_sub.add_parser("exec", help="Execute notebooks")
    nb_sub.add_parser("publish", help="Publish notebooks to docs")
    nb_sub.add_parser("status", help="Show notebook sync status")

    # pipeio registry {scan,validate}
    reg_p = sub.add_parser("registry", help="Pipeline registry")
    reg_p.add_argument("--root", dest="root", help="Project root")
    reg_sub = reg_p.add_subparsers(dest="registry_command")

    reg_scan = reg_sub.add_parser("scan", help="Scan filesystem for flows")
    reg_scan.add_argument("--pipelines-dir", help="Pipelines directory path")
    reg_scan.add_argument("--output", help="Output YAML path")

    reg_val = reg_sub.add_parser("validate", help="Validate registry consistency")
    reg_val.add_argument("--registry", help="Registry YAML path")

    # pipeio contracts {validate}
    con_p = sub.add_parser("contracts", help="Pipeline contracts")
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
        flow_p.print_help()
        return 0

    if args.command == "registry":
        if args.registry_command == "scan":
            return _cmd_registry_scan(args)
        if args.registry_command == "validate":
            return _cmd_registry_validate(args)
        reg_p.print_help()
        return 0

    if args.command == "nb":
        print(f"pipeio nb {args.nb_command}: not yet implemented — requires pipeio[notebook]")
        return 1

    if args.command == "contracts":
        print(f"pipeio contracts: not yet implemented — see docs/specs/pipeio/contracts.md")
        return 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
