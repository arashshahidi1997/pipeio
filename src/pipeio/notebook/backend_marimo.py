"""Marimo reactive notebook backend.

Single-file ``.py`` format with DAG-based execution via ``marimo`` CLI.
The backend is always registered; the ``marimo`` command is resolved lazily
(or passed in), so a server env without marimo still classifies and drives
marimo notebooks through the project env.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


# Pattern to detect marimo notebooks: `import marimo` at module level
_MARIMO_IMPORT_RE = re.compile(r"^\s*import\s+marimo\b", re.MULTILINE)
_MARIMO_APP_RE = re.compile(r"marimo\.App\(", re.MULTILINE)
# Pattern to split on @app.cell decorators
_CELL_DECORATOR_RE = re.compile(
    r"^@app\.cell(?:\(.*?\))?\s*$",
    re.MULTILINE,
)


def is_marimo_source(py_path: Path) -> bool:
    """Return True if *py_path* has the marimo notebook signature.

    Pure text check: works without marimo installed, so callers that only
    need to classify files (e.g. a server env launching marimo through a
    separate ``python_bin``) still see marimo notebooks.
    """
    try:
        head = py_path.read_text(encoding="utf-8", errors="ignore")[:4096]
        return bool(_MARIMO_IMPORT_RE.search(head) and _MARIMO_APP_RE.search(head))
    except Exception:
        return False


class MarimoBackend:
    """Marimo reactive notebook backend."""

    name = "marimo"

    def __init__(self, marimo_cmd: "list[str] | None" = None) -> None:
        # The command is resolved lazily: the backend must register even when
        # marimo is absent from this (server) env, since detection is a pure
        # text check and callers usually run marimo through the project env
        # (``pixi run python -m marimo``) rather than this interpreter.
        self._cmd = list(marimo_cmd) if marimo_cmd else None

    @property
    def _marimo_cmd(self) -> list[str]:
        if getattr(self, "_cmd", None) is None:
            self._cmd = self._find_marimo()
        return self._cmd

    @_marimo_cmd.setter
    def _marimo_cmd(self, value: list[str]) -> None:
        self._cmd = list(value)

    def _find_marimo(self) -> list[str]:
        """Return the command prefix for invoking marimo.

        Falls back to bare ``marimo``; a missing binary then surfaces as a
        "marimo command not found" error from the operation that needs it.
        """
        # Try `python -m marimo` first (works in the current env)
        try:
            subprocess.run(
                # cold imports on network storage can take ~10 s
                [sys.executable, "-m", "marimo", "--version"],
                capture_output=True, check=True, timeout=60,
            )
            return [sys.executable, "-m", "marimo"]
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        # Bare `marimo` on PATH (or the "not found" error later)
        return ["marimo"]

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def detect(self, py_path: Path) -> bool:
        """Return True if *py_path* is a marimo-format notebook."""
        return is_marimo_source(py_path)

    # ------------------------------------------------------------------
    # Output paths
    # ------------------------------------------------------------------

    def output_paths(self, py_path: Path) -> dict[str, Path]:
        """Marimo is single-file — no paired outputs."""
        return {}

    # ------------------------------------------------------------------
    # Sync
    # ------------------------------------------------------------------

    def sync(
        self,
        py_path: Path,
        *,
        direction: str = "py2nb",
        formats: list[str] | None = None,
        force: bool = False,
        kernel: str = "",
        python_bin: "str | list[str] | None" = None,
    ) -> dict[str, Any]:
        """No-op: marimo is a single-file format."""
        return {
            "synced": False,
            "skipped": True,
            "direction": direction,
            "source": str(py_path),
            "reason": "Marimo notebooks are single-file (no sync needed)",
            "format": "marimo",
        }

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute(
        self,
        py_path: Path,
        *,
        kernel: str = "",
        params: dict[str, Any] | None = None,
        timeout: int = 600,
        cwd: Path | None = None,
    ) -> dict[str, Any]:
        """Execute via ``marimo run``."""
        if not py_path.exists():
            return {"executed": False, "error": f"Not found: {py_path}"}

        cmd = [*self._marimo_cmd, "run", str(py_path), "--headless"]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(cwd) if cwd else None,
            )
            if result.returncode == 0:
                return {
                    "executed": True,
                    "path": str(py_path),
                    "format": "marimo",
                    "stdout": result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout,
                }
            else:
                return {
                    "executed": False,
                    "error": result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr,
                    "path": str(py_path),
                }
        except subprocess.TimeoutExpired:
            return {"executed": False, "error": f"Timeout after {timeout}s", "path": str(py_path)}

    # ------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------

    def validate(self, py_path: Path) -> dict[str, Any]:
        """Validate via ``marimo check``."""
        if not py_path.exists():
            return {"valid": False, "error": f"Not found: {py_path}"}

        try:
            result = subprocess.run(
                [*self._marimo_cmd, "check", str(py_path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            return {
                "valid": result.returncode == 0,
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip() if result.stderr else "",
                "format": "marimo",
            }
        except subprocess.TimeoutExpired:
            return {"valid": False, "error": "marimo check timed out"}
        except FileNotFoundError:
            return {"valid": False, "error": "marimo command not found"}

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export(
        self,
        py_path: Path,
        *,
        output_format: str,
        output_path: Path,
        marimo_cmd: "list[str] | None" = None,
        timeout: int = 600,
        cwd: Path | None = None,
    ) -> dict[str, Any]:
        """Export via ``marimo export``.

        ``html`` (and ``ipynb``) exports execute the notebook, so *marimo_cmd*
        should run in the env that has the notebook's imports (e.g.
        ``["pixi", "run", "python", "-m", "marimo"]``); default is this
        backend's own command. *cwd* defaults to the notebook's directory.
        """
        if not py_path.exists():
            return {"exported": False, "error": f"Not found: {py_path}"}

        # Map pipeio format names to marimo export subcommands
        fmt_map = {
            "html": "html",
            "markdown": "md",
            "md": "md",
            "myst": "md",
            "ipynb": "ipynb",
        }
        marimo_fmt = fmt_map.get(output_format, output_format)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = list(marimo_cmd) if marimo_cmd else self._marimo_cmd

        try:
            result = subprocess.run(
                [*cmd, "export", marimo_fmt, str(py_path),
                 "-o", str(output_path)],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(cwd or py_path.parent),
            )
            if result.returncode == 0 and output_path.exists():
                return {"exported": True, "path": str(output_path), "format": output_format}
            return {
                "exported": False,
                "error": (result.stderr.strip() or result.stdout.strip())[-2000:]
                or f"marimo export exited {result.returncode}",
                "command": cmd,
            }
        except subprocess.TimeoutExpired:
            return {"exported": False, "error": f"marimo export timed out after {timeout}s"}
        except OSError as exc:  # not found / not executable
            return {"exported": False, "error": f"marimo command failed to start: {exc}", "command": cmd}

    # ------------------------------------------------------------------
    # Cell splitting
    # ------------------------------------------------------------------

    def split_cells(self, source: str) -> list[tuple[str, str]]:
        """Split marimo source into ``[(kind, content), ...]``.

        Parses ``@app.cell`` decorated functions and extracts their bodies.
        ``mo.md(...)`` calls are classified as ``'markdown'`` cells.
        """
        cells: list[tuple[str, str]] = []

        try:
            tree = ast.parse(source)
        except SyntaxError:
            return [("code", source)]

        for node in ast.iter_child_nodes(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            # Check if decorated with @app.cell
            is_cell = False
            for dec in node.decorator_list:
                if isinstance(dec, ast.Attribute):
                    if (isinstance(dec.value, ast.Name) and dec.value.id == "app"
                            and dec.attr == "cell"):
                        is_cell = True
                elif isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
                    if (isinstance(dec.func.value, ast.Name) and dec.func.value.id == "app"
                            and dec.func.attr == "cell"):
                        is_cell = True
                if is_cell:
                    break
            if not is_cell:
                continue

            # Extract function body as source
            body_lines = []
            for stmt in node.body:
                try:
                    body_lines.append(ast.unparse(stmt))
                except Exception:
                    body_lines.append(ast.get_source_segment(source, stmt) or "")

            body = "\n".join(body_lines)

            # Classify: if body is a single mo.md() call, it's markdown
            if len(node.body) == 1:
                stmt = node.body[0]
                if _is_mo_md_call(stmt):
                    # Extract the markdown string content
                    md_content = _extract_mo_md_content(stmt, source)
                    cells.append(("markdown", md_content))
                    continue

            cells.append(("code", body))

        return cells if cells else [("code", source)]

    # ------------------------------------------------------------------
    # Template
    # ------------------------------------------------------------------

    def template(
        self,
        *,
        name: str,
        flow: str,
        kind: str,
        description: str,
        config_path: str = "",
        groups: list[str] | None = None,
        output_dir: str = "",
        compute_lib: str = "",
    ) -> str:
        """Generate a marimo notebook template."""
        title = name.replace("_", " ").title()
        desc = description or f"{kind.title()} notebook for {flow}"

        lines = [
            "import marimo",
            "",
            'app = marimo.App(width="medium")',
            "",
            "",
            "@app.cell",
            "def title(mo):",
            f'    mo.md("""',
            f"    # {title}",
            f"    {desc}",
            '    """)',
            "",
            "",
            "@app.cell",
            "def setup():",
            "    from pathlib import Path",
            "",
        ]

        if config_path:
            lines.append(f'    config_path = Path("{config_path}")')

        if compute_lib:
            lines.append(f"    import {compute_lib}")

        lines.extend([
            "    return (Path,)",
            "",
            "",
            "@app.cell",
            "def data_loading(Path):",
        ])

        if output_dir:
            lines.append(f'    output_dir = Path("{output_dir}")')
        else:
            lines.append("    # Load pipeline outputs for exploration")

        if groups:
            lines.append(f"    # Available registry groups: {', '.join(groups)}")

        lines.extend([
            "    return ()",
            "",
            "",
            "@app.cell",
            "def visualization():",
            "    # Plots and statistical summaries",
            "    return ()",
            "",
            "",
            "@app.cell",
            "def findings(mo):",
            '    mo.md("""',
            "    ## Findings",
            "    Summarize results here.",
            '    """)',
            "",
            "",
            'if __name__ == "__main__":',
            "    app.run()",
            "",
        ])

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_mo_md_call(stmt: ast.stmt) -> bool:
    """Check if an AST statement is a mo.md(...) call."""
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
        func = stmt.value.func
        if isinstance(func, ast.Attribute):
            return (isinstance(func.value, ast.Name) and func.value.id == "mo"
                    and func.attr == "md")
    # Also handle: return (mo.md(...),) pattern
    if isinstance(stmt, ast.Return) and isinstance(stmt.value, ast.Tuple):
        if len(stmt.value.elts) == 1:
            elt = stmt.value.elts[0]
            if isinstance(elt, ast.Call) and isinstance(elt.func, ast.Attribute):
                return (isinstance(elt.func.value, ast.Name) and elt.func.value.id == "mo"
                        and elt.func.attr == "md")
    return False


def _extract_mo_md_content(stmt: ast.stmt, source: str) -> str:
    """Extract the string argument from a mo.md() call."""
    call = None
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
        call = stmt.value
    elif isinstance(stmt, ast.Return) and isinstance(stmt.value, ast.Tuple):
        if stmt.value.elts:
            call = stmt.value.elts[0]

    if call and isinstance(call, ast.Call) and call.args:
        arg = call.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
        if isinstance(arg, ast.JoinedStr):
            # f-string — just return the unparsed version
            return ast.unparse(arg)
    return ast.get_source_segment(source, stmt) or ""
