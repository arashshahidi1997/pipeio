"""Static analysis of percent-format .py notebooks.

Parses a jupytext percent-format script and returns structured metadata:
imports, RunCard dataclass fields, PipelineContext usage, section headers,
and cogpy function calls.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Cell splitting
# ---------------------------------------------------------------------------

_CELL_MARKER = re.compile(r"^# %%(?:\s+\[(\w+)\])?[^\n]*\n?", re.MULTILINE)
_COMMENTED_IMPORT_RE = re.compile(r"^#\s+((?:import|from)\s+\S.*)")


def _split_cells(source: str) -> list[tuple[str, str]]:
    """Split percent-format source into [(kind, content), ...].

    kind is 'code' or 'markdown'.  Content is everything after the marker
    line up to (but not including) the next marker line.
    """
    markers = list(_CELL_MARKER.finditer(source))
    if not markers:
        return [("code", source)]

    cells = []
    for i, m in enumerate(markers):
        kind = m.group(1) or "code"
        content_start = m.end()
        content_end = markers[i + 1].start() if i + 1 < len(markers) else len(source)
        cells.append((kind, source[content_start:content_end]))
    return cells


# ---------------------------------------------------------------------------
# Markdown extraction
# ---------------------------------------------------------------------------

def _extract_sections(content: str) -> list[str]:
    """Extract markdown headers from a markdown cell.

    Each line in a markdown cell is prefixed with '# '.  Strip that prefix
    and collect lines that are themselves markdown headers (starting with #).
    """
    headers = []
    for line in content.splitlines():
        if line.startswith("# "):
            md_line = line[2:]
            if md_line.startswith("#"):
                headers.append(md_line)
    return headers


# ---------------------------------------------------------------------------
# Import extraction
# ---------------------------------------------------------------------------

def _extract_imports(tree: ast.Module) -> list[dict[str, Any]]:
    """Extract top-level import statements from an AST."""
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append({
                    "kind": "import",
                    "module": alias.name,
                    "alias": alias.asname,
                    "names": None,
                })
        elif isinstance(node, ast.ImportFrom):
            names = [
                {"name": a.name, "alias": a.asname}
                for a in node.names
            ]
            imports.append({
                "kind": "from",
                "module": node.module or "",
                "alias": None,
                "names": names,
            })
    return imports


def _extract_commented_imports(content: str) -> list[str]:
    """Extract commented-out import statements (pending/not-yet-implemented modules)."""
    pending = []
    for line in content.splitlines():
        m = _COMMENTED_IMPORT_RE.match(line.strip())
        if m:
            pending.append(m.group(1))
    return pending


# ---------------------------------------------------------------------------
# RunCard extraction
# ---------------------------------------------------------------------------

def _extract_run_card(tree: ast.Module) -> list[dict[str, Any]]:
    """Extract @dataclass class fields (RunCard and similar) from an AST."""
    fields = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        is_dataclass = any(
            (isinstance(d, ast.Name) and d.id == "dataclass")
            or (isinstance(d, ast.Attribute) and d.attr == "dataclass")
            for d in node.decorator_list
        )
        if not is_dataclass:
            continue
        for item in node.body:
            if not isinstance(item, ast.AnnAssign):
                continue
            name = item.target.id if isinstance(item.target, ast.Name) else None
            if name is None:
                continue
            fields.append({
                "class": node.name,
                "field": name,
                "type": ast.unparse(item.annotation) if item.annotation else None,
                "default": ast.unparse(item.value) if item.value else None,
            })
    return fields


# ---------------------------------------------------------------------------
# PipelineContext extraction
# ---------------------------------------------------------------------------

def _extract_pipeline_context(tree: ast.Module) -> list[dict[str, Any]]:
    """Extract PipelineContext and ctx.session/stage calls from an AST."""
    usages = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue

        pos_args = [ast.unparse(a) for a in node.args]
        kwargs = {
            kw.arg: ast.unparse(kw.value)
            for kw in node.keywords
            if kw.arg is not None
        }

        # PipelineContext.from_registry / PipelineContext.from_config / PipelineContext(...)
        if isinstance(func.value, ast.Name) and func.value.id == "PipelineContext":
            usages.append({
                "call": f"PipelineContext.{func.attr}",
                "args": pos_args,
                "kwargs": kwargs,
            })
        # ctx.session(...) / ctx.stage(...) — any variable's .session/.stage call
        elif func.attr in ("session", "stage", "path", "expand", "have", "groups", "products"):
            receiver = ast.unparse(func.value)
            usages.append({
                "call": f"{receiver}.{func.attr}",
                "args": pos_args,
                "kwargs": kwargs,
            })
    return usages


# ---------------------------------------------------------------------------
# cogpy call extraction
# ---------------------------------------------------------------------------

def _get_root_name(node: ast.expr) -> str | None:
    """Return the root Name.id of an attribute chain or direct Name."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _get_root_name(node.value)
    return None


def _build_cogpy_names(imports: list[dict[str, Any]]) -> dict[str, str]:
    """Map local names → cogpy module paths for all cogpy imports.

    Returns {local_name: cogpy.path} so that call detection can match
    any attribute access rooted at a cogpy-sourced name.
    """
    names: dict[str, str] = {}
    for imp in imports:
        if not imp["module"].startswith("cogpy"):
            continue
        if imp["kind"] == "import":
            local = imp["alias"] or imp["module"].split(".")[0]
            names[local] = imp["module"]
        elif imp["kind"] == "from" and imp["names"]:
            for entry in imp["names"]:
                local = entry["alias"] or entry["name"]
                names[local] = f"{imp['module']}.{entry['name']}"
    return names


def _extract_cogpy_calls(
    tree: ast.Module,
    cogpy_names: dict[str, str],
) -> list[str]:
    """Collect unique dotted call expressions rooted at a cogpy name."""
    calls: list[str] = []
    seen: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        root = _get_root_name(node.func)
        if root and root in cogpy_names:
            call_str = ast.unparse(node.func)
            if call_str not in seen:
                seen.add(call_str)
                calls.append(call_str)
    return calls


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def analyze_notebook(py_path: Path) -> dict[str, Any]:
    """Parse a percent-format .py notebook and return structured metadata.

    Args:
        py_path: Path to the percent-format ``.py`` notebook file.

    Returns:
        dict with keys:

        - ``nb_path``: absolute path string
        - ``imports``: list of import dicts (kind, module, alias, names)
        - ``run_card``: list of @dataclass field dicts (class, field, type, default)
        - ``pipeline_context``: list of PipelineContext/session call dicts
        - ``sections``: list of markdown header strings (e.g. "## Setup")
        - ``cogpy_functions``: deduplicated list of cogpy dotted call expressions
        - ``pending_modules``: commented-out import statements
        - ``error``: present only if parsing failed
    """
    if not py_path.exists():
        return {"error": f"Notebook not found: {py_path}"}

    try:
        source = py_path.read_text(encoding="utf-8")
    except OSError as exc:
        return {"error": f"Cannot read notebook: {exc}"}

    cells = _split_cells(source)

    all_imports: list[dict[str, Any]] = []
    all_run_card: list[dict[str, Any]] = []
    all_pipeline_context: list[dict[str, Any]] = []
    all_sections: list[str] = []
    all_cogpy_calls: list[str] = []
    all_pending: list[str] = []

    # Two-pass: collect imports first so cogpy_names is complete for call detection
    code_cells: list[str] = []
    for kind, content in cells:
        if kind == "markdown":
            all_sections.extend(_extract_sections(content))
        else:
            code_cells.append(content)
            all_pending.extend(_extract_commented_imports(content))
            try:
                tree = ast.parse(content)
                all_imports.extend(_extract_imports(tree))
            except SyntaxError:
                pass

    # Build cogpy name map from all collected imports
    cogpy_names = _build_cogpy_names(all_imports)

    # Second pass over code cells for RunCard, PipelineContext, cogpy calls
    for content in code_cells:
        try:
            tree = ast.parse(content)
        except SyntaxError:
            continue
        all_run_card.extend(_extract_run_card(tree))
        all_pipeline_context.extend(_extract_pipeline_context(tree))
        all_cogpy_calls.extend(_extract_cogpy_calls(tree, cogpy_names))

    # Deduplicate cogpy calls preserving first-occurrence order
    seen: set[str] = set()
    cogpy_functions: list[str] = []
    for call in all_cogpy_calls:
        if call not in seen:
            seen.add(call)
            cogpy_functions.append(call)

    # Deduplicate pending imports
    pending_seen: set[str] = set()
    pending: list[str] = []
    for p in all_pending:
        if p not in pending_seen:
            pending_seen.add(p)
            pending.append(p)

    return {
        "nb_path": str(py_path),
        "imports": all_imports,
        "run_card": all_run_card,
        "pipeline_context": all_pipeline_context,
        "sections": all_sections,
        "cogpy_functions": cogpy_functions,
        "pending_modules": pending,
    }
