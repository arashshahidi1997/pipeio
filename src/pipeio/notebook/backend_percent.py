"""Jupytext percent-format notebook backend.

Wraps existing lifecycle functions into the ``NotebookBackend`` interface.
No behavior changes — this is a refactoring extraction.
"""

from __future__ import annotations

import ast
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any


_CELL_MARKER = re.compile(r"^# %%(?:\s+\[(\w+)\])?[^\n]*\n?", re.MULTILINE)


class PercentBackend:
    """Jupytext percent-format backend."""

    name = "percent"

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def detect(self, py_path: Path) -> bool:
        """Return True if *py_path* looks like a jupytext percent-format notebook."""
        try:
            head = py_path.read_text(encoding="utf-8", errors="ignore")[:4096]
            return "# %%" in head
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Output paths
    # ------------------------------------------------------------------

    def output_paths(self, py_path: Path) -> dict[str, Path]:
        """Compute ipynb and myst output paths from a .py source path.

        Layout-aware: if ``.py`` is in a ``.src/`` directory, ``.ipynb``
        goes to the parent workspace dir and ``.md`` goes to ``.myst/``.
        """
        name = py_path.stem
        if py_path.parent.name == ".src":
            workspace_dir = py_path.parent.parent
            ipynb_path = workspace_dir / f"{name}.ipynb"
            myst_dir = workspace_dir / ".myst"
            myst_path = myst_dir / f"{name}.md"
        else:
            ipynb_path = py_path.with_suffix(".ipynb")
            myst_path = py_path.with_suffix(".md")
        return {"ipynb": ipynb_path, "myst": myst_path}

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
        """Sync between .py and paired formats via jupytext."""
        if formats is None:
            formats = ["ipynb", "myst"]

        _require_jupytext(python_bin=python_bin)

        if direction == "nb2py":
            return self._sync_nb2py(py_path, force=force, python_bin=python_bin)
        elif direction == "py2nb":
            return self._sync_py2nb(
                py_path, formats=formats, force=force,
                kernel=kernel, python_bin=python_bin,
            )
        else:
            return {"error": f"Unknown direction: {direction!r}. Use 'py2nb' or 'nb2py'."}

    def _sync_py2nb(
        self,
        py_path: Path,
        *,
        formats: list[str],
        force: bool = False,
        kernel: str = "",
        python_bin: "str | list[str] | None" = None,
    ) -> dict[str, Any]:
        if not py_path.exists():
            return {"error": f"Source not found: {py_path}"}

        py_mtime = py_path.stat().st_mtime
        generated: list[str] = []
        kernel_args: tuple[str, ...] = ("--set-kernel", kernel) if kernel else ()
        paths = self.output_paths(py_path)

        if "ipynb" in formats and "ipynb" in paths:
            ipynb_path = paths["ipynb"]
            if force or not ipynb_path.exists() or ipynb_path.stat().st_mtime < py_mtime:
                _jupytext(py_path, "--to", "notebook", "--output", str(ipynb_path),
                           *kernel_args, python_bin=python_bin)
                generated.append(str(ipynb_path))

        if "myst" in formats and "myst" in paths:
            myst_path = paths["myst"]
            myst_path.parent.mkdir(parents=True, exist_ok=True)
            if force or not myst_path.exists() or myst_path.stat().st_mtime < py_mtime:
                _jupytext(py_path, "--to", "myst", "--output", str(myst_path),
                           python_bin=python_bin)
                generated.append(str(myst_path))

        return {
            "synced": bool(generated),
            "skipped": not generated,
            "direction": "py2nb",
            "source": str(py_path),
            "generated": generated,
            **({"kernel": kernel} if kernel else {}),
        }

    def _sync_nb2py(
        self,
        py_path: Path,
        *,
        force: bool = False,
        python_bin: "str | list[str] | None" = None,
    ) -> dict[str, Any]:
        paths = self.output_paths(py_path)
        ipynb_path = paths.get("ipynb")
        if ipynb_path is None or not ipynb_path.exists():
            return {"error": f"Paired notebook not found: {ipynb_path}"}

        if not force and py_path.exists():
            if py_path.stat().st_mtime >= ipynb_path.stat().st_mtime:
                return {
                    "synced": False,
                    "skipped": True,
                    "direction": "nb2py",
                    "source": str(ipynb_path),
                    "reason": ".py is already newer than .ipynb",
                }

        py_path.parent.mkdir(parents=True, exist_ok=True)
        _jupytext(ipynb_path, "--to", "py:percent", "--output", str(py_path),
                   python_bin=python_bin)

        return {
            "synced": True,
            "skipped": False,
            "direction": "nb2py",
            "source": str(ipynb_path),
            "updated": [str(py_path)],
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
        """Execute via nbconvert (batch) on the paired .ipynb."""
        paths = self.output_paths(py_path)
        ipynb_path = paths.get("ipynb")
        if ipynb_path is None or not ipynb_path.exists():
            return {"error": f"Paired .ipynb not found: {ipynb_path}. Run sync first."}

        try:
            cmd = [
                "jupyter", "nbconvert",
                "--to", "notebook",
                "--execute",
                "--inplace",
                str(ipynb_path),
            ]
            if timeout:
                cmd.extend(["--ExecutePreprocessor.timeout", str(timeout)])
            subprocess.run(cmd, check=True, capture_output=True, text=True,
                           timeout=timeout + 30, cwd=str(cwd) if cwd else None)
            return {"executed": True, "path": str(ipynb_path)}
        except subprocess.CalledProcessError as exc:
            return {"executed": False, "error": exc.stderr or str(exc), "path": str(ipynb_path)}
        except subprocess.TimeoutExpired:
            return {"executed": False, "error": f"Timeout after {timeout}s", "path": str(ipynb_path)}

    # ------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------

    def validate(self, py_path: Path) -> dict[str, Any]:
        """Validate percent-format notebook: syntax check + import isolation."""
        if not py_path.exists():
            return {"valid": False, "error": f"Not found: {py_path}"}

        source = py_path.read_text(encoding="utf-8")
        cells = self.split_cells(source)
        issues: list[dict[str, Any]] = []

        defined_names: set[str] = set()
        import_cells = 0
        code_cells_before_first_import = 0
        found_first_import = False

        for i, (kind, content) in enumerate(cells):
            if kind != "code":
                continue
            stripped = content.strip()
            if not stripped:
                continue

            # Syntax check
            try:
                tree = ast.parse(stripped)
            except SyntaxError as exc:
                issues.append({
                    "cell": i,
                    "severity": "error",
                    "message": f"SyntaxError: {exc.msg} (line {exc.lineno})",
                })
                continue

            # Import isolation check
            has_imports = any(
                isinstance(n, (ast.Import, ast.ImportFrom))
                for n in ast.iter_child_nodes(tree)
            )
            has_non_imports = any(
                not isinstance(n, (ast.Import, ast.ImportFrom, ast.Expr))
                or (isinstance(n, ast.Expr) and not isinstance(n.value, ast.Constant))
                for n in ast.iter_child_nodes(tree)
            )
            if has_imports:
                import_cells += 1
                if not found_first_import:
                    found_first_import = True
                if has_non_imports:
                    issues.append({
                        "cell": i,
                        "severity": "warning",
                        "message": "Cell mixes imports with executable code",
                    })

            # Variable shadowing check
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, (ast.Assign, ast.AnnAssign)):
                    targets = []
                    if isinstance(node, ast.Assign):
                        for t in node.targets:
                            if isinstance(t, ast.Name):
                                targets.append(t.id)
                    elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                        targets.append(node.target.id)
                    for name in targets:
                        if name in defined_names:
                            issues.append({
                                "cell": i,
                                "severity": "warning",
                                "message": f"Variable '{name}' redefined (first defined in earlier cell)",
                            })
                        defined_names.add(name)

        return {
            "valid": not any(i["severity"] == "error" for i in issues),
            "issues": issues,
            "issue_count": len(issues),
            "format": "percent",
        }

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export(
        self,
        py_path: Path,
        *,
        output_format: str,
        output_path: Path,
    ) -> dict[str, Any]:
        """Export via nbconvert on the paired .ipynb."""
        paths = self.output_paths(py_path)

        if output_format == "myst":
            myst_path = paths.get("myst")
            if myst_path and myst_path.exists():
                output_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(myst_path, output_path)
                return {"exported": True, "path": str(output_path), "format": "myst"}
            return {"exported": False, "error": f"MyST file not found: {myst_path}"}

        # HTML or other formats via nbconvert
        ipynb_path = paths.get("ipynb")
        if ipynb_path is None or not ipynb_path.exists():
            return {"exported": False, "error": f"Paired .ipynb not found: {ipynb_path}"}

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [
                    "jupyter", "nbconvert",
                    "--to", output_format,
                    str(ipynb_path),
                    "--output", str(output_path),
                ],
                check=True, capture_output=True, text=True,
            )
            return {"exported": True, "path": str(output_path), "format": output_format}
        except subprocess.CalledProcessError as exc:
            return {"exported": False, "error": exc.stderr or str(exc)}

    # ------------------------------------------------------------------
    # Cell splitting
    # ------------------------------------------------------------------

    def split_cells(self, source: str) -> list[tuple[str, str]]:
        """Split percent-format source into ``[(kind, content), ...]``."""
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
        """Generate a percent-format notebook template.

        This produces the same output as the original ``_nb_template()``
        in mcp.py — delegated here for format-awareness.
        """
        title = name.replace("_", " ").title()
        L: list[str] = []

        # --- Header ---
        L.extend([
            "# ---",
            "# jupyter:",
            "#   jupytext:",
            "#     text_representation:",
            "#       format_name: percent",
            "# ---",
            "",
        ])

        # --- Title cell ---
        L.extend([
            "# %% [markdown]",
            f"# # {title}",
            "#",
            f"# {description}" if description else f"# {kind.title()} notebook for {flow}",
            "",
        ])

        # --- Setup cell ---
        L.extend([
            "# %% [markdown]",
            "# ## Setup",
            "",
            "# %%",
            "from pathlib import Path",
            "",
            "import yaml",
            "",
        ])

        if config_path:
            L.append(f'config_path = Path("{config_path}")')
            L.append("")

        if compute_lib:
            L.extend([f"import {compute_lib}", ""])

        # --- Data Loading cell ---
        L.extend([
            "# %% [markdown]",
            "# ## Data Loading",
            "#",
            "# Load pipeline outputs for exploration.",
            "",
            "# %%",
        ])

        if output_dir:
            L.append(f'output_dir = Path("{output_dir}")')
        if groups:
            L.append(f"# Available registry groups: {', '.join(groups)}")
        L.append("")

        # --- Visualization cell ---
        L.extend([
            "# %% [markdown]",
            "# ## Visualization",
            "",
            "# %%",
            "",
        ])

        # --- Summary / Findings cell ---
        if kind in ("demo", "validate"):
            L.extend([
                "# %% [markdown]",
                "# ## Summary",
                "#",
                "# Summarize results here.",
                "",
                "# %%",
                "",
            ])
        else:
            L.extend([
                "# %% [markdown]",
                "# ## Findings",
                "#",
                "# Summarize results here. These feed into theory.md.",
                "",
                "# %%",
                "",
            ])

        return "\n".join(L)


# ---------------------------------------------------------------------------
# Helpers (extracted from lifecycle.py)
# ---------------------------------------------------------------------------

def _python_prefix(python_bin: "str | list[str] | None") -> list[str]:
    if python_bin is None:
        return []
    if isinstance(python_bin, list):
        return list(python_bin)
    return [python_bin]


def _require_jupytext(python_bin: "str | list[str] | None" = None) -> None:
    if python_bin:
        prefix = _python_prefix(python_bin)
        try:
            subprocess.run(
                [*prefix, "-m", "jupytext", "--version"],
                capture_output=True, check=True, timeout=15,
            )
            return
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            raise ImportError(
                f"jupytext not found via {python_bin}. "
                "Install with: pip install pipeio[notebook]"
            )
    try:
        import jupytext  # noqa: F401
    except ImportError:
        raise ImportError(
            "Notebook operations require jupytext. "
            "Install with: pip install pipeio[notebook]"
        )


def _jupytext(source: Path, *args: str, python_bin: "str | list[str] | None" = None) -> None:
    if python_bin:
        prefix = _python_prefix(python_bin)
        cmd = [*prefix, "-m", "jupytext", str(source), *args]
    else:
        cmd = ["jupytext", str(source), *args]
    subprocess.run(cmd, check=True)
