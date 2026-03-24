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


def _find_registry(root: Path) -> Path | None:
    """Locate the pipeline registry, checking .projio/pipeio/ first."""
    for candidate in (
        root / ".projio" / "pipeio" / "registry.yml",
        root / ".pipeio" / "registry.yml",
    ):
        if candidate.exists():
            return candidate
    return None


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


def _cmd_flow_list(args: argparse.Namespace) -> int:
    from pipeio.registry import PipelineRegistry

    root = Path(args.root) if args.root else _find_root()
    reg_path = _find_registry(root)
    if reg_path is None:
        print("No registry found. Run 'pipeio init' first.", file=sys.stderr)
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
    if hasattr(args, "output") and args.output:
        output = Path(args.output)
    else:
        output = _pipeio_dir(root) / "registry.yml"
    registry.to_yaml(output)
    print(f"Written: {output} ({len(pipes)} pipes, {len(flows)} flows)")
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
    from pipeio.notebook.lifecycle import nb_sync

    root = Path(args.root) if args.root else _find_root()
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
    try:
        collected = docs_collect(root)
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
    flow_list.add_argument("--pipe", help="Filter by pipe name")

    flow_new = flow_sub.add_parser("new", help="Scaffold a new flow")
    flow_new.add_argument("pipe", help="Pipeline name")
    flow_new.add_argument("flow", help="Flow name")

    # pipeio nb {pair,sync,exec,publish,status}
    nb_p = sub.add_parser("nb", help="Notebook lifecycle")
    nb_p.add_argument("--root", dest="root", help="Project root")
    nb_sub = nb_p.add_subparsers(dest="nb_command")

    nb_pair_p = nb_sub.add_parser("pair", help="Pair .py notebooks with ipynb/myst")
    nb_pair_p.add_argument("--force", action="store_true", help="Re-create existing pairs")

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

    # pipeio docs {collect,nav}
    docs_p = sub.add_parser("docs", help="Pipeline documentation")
    docs_p.add_argument("--root", dest="root", help="Project root")
    docs_sub = docs_p.add_subparsers(dest="docs_command")
    docs_sub.add_parser("collect", help="Collect flow docs and notebook outputs into docs/pipelines/")
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
        if args.nb_command == "status":
            return _cmd_nb_status(args)
        if args.nb_command == "pair":
            return _cmd_nb_pair(args)
        if args.nb_command == "sync":
            return _cmd_nb_sync(args)
        if args.nb_command == "exec":
            return _cmd_nb_exec(args)
        if args.nb_command == "publish":
            return _cmd_nb_publish(args)
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


if __name__ == "__main__":
    sys.exit(main())
