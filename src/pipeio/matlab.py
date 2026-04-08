"""MATLAB rule wiring utilities for Snakemake pipelines.

Provides ``matlab2shell()`` to compose ``matlab -batch`` commands suitable
for Snakemake ``shell:`` directives.

Typical usage in a Snakefile::

    from pipeio.matlab import matlab2shell

    rule sleepstates:
        shell:
            matlab2shell(
                "myFunction(pwd, '{wildcards.sub}')",
                matlab_bin="/usr/local/MATLAB/R2023a/bin/matlab",
            ) + " >> {log} 2>&1"

The *matlab_bin* parameter can be sourced from projio's ``runtime.matlab_bin``
config key via the MCP server — see the pipeio MATLAB wiring docs.
"""

from __future__ import annotations

import os

_DEFAULT_MATLAB = "matlab"


def _startup_snippet(
    *,
    startup_script: str | None = "code/startup.m",
    reset_path: bool = True,
) -> str:
    """Build the MATLAB prelude that optionally resets the path and runs startup."""
    parts: list[str] = []

    if reset_path:
        parts.append("restoredefaultpath; rehash toolboxcache;")

    if startup_script:
        # Use run() so it works with absolute or relative paths
        parts.append(f"run('{startup_script}');")

    return " ".join(parts)


def matlab2shell(
    command: str,
    *,
    matlab_bin: str | None = None,
    startup_script: str | None = "code/startup.m",
    reset_path: bool = True,
) -> str:
    """Compose a ``matlab -batch`` shell command string.

    Parameters
    ----------
    command
        MATLAB expression to execute (e.g. ``"myFunc(arg1, arg2)"``).
    matlab_bin
        Path to the MATLAB binary. Falls back to the ``MATLAB_BIN``
        environment variable, then to ``"matlab"`` (i.e. whatever is on
        ``$PATH``).
    startup_script
        Path to a startup ``.m`` file that is ``run()`` before *command*.
        Set to ``None`` to skip.  Default: ``"code/startup.m"``.
    reset_path
        If ``True`` (default), prepend ``restoredefaultpath; rehash toolboxcache;``
        to clear MATLAB's path before running startup.

    Returns
    -------
    str
        A shell-ready command string suitable for a Snakemake ``shell:``
        directive.
    """
    matlab = matlab_bin or os.environ.get("MATLAB_BIN") or _DEFAULT_MATLAB
    prelude = _startup_snippet(startup_script=startup_script, reset_path=reset_path)

    if prelude:
        batch_expr = f"{prelude} {command}"
    else:
        batch_expr = command

    return f'{matlab} -batch "{batch_expr}"'
