"""Discovery and launch of local marimo notebook servers.

Every marimo server started **without** a token (``--no-token``) writes a JSON
descriptor into marimo's XDG state directory
(``~/.local/state/marimo/servers/<host>_<port>.json``) and removes it on
shutdown.  That descriptor is the same source the ``marimo-pair`` agent skill
reads, so servers launched from here are auto-discoverable.

The descriptor carries ``host``/``port``/``pid`` but *not* the notebook being
served, so mapping a server back to a notebook needs a second probe:

1. the process command line (``/proc/<pid>/cmdline`` or psutil) — cheap and
   works before anyone has opened the browser;
2. the server's ``/api/sessions`` endpoint — catches notebooks opened through
   the UI after the server started.

Both probes are injectable, so everything here is testable against a fake
state directory without marimo installed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

DEFAULT_HOST = "0.0.0.0"

#: Bind addresses that are not usable as-is in a URL.
_WILDCARD_HOSTS = {"", "0.0.0.0", "::", "[::]"}


# ---------------------------------------------------------------------------
# Registry discovery
# ---------------------------------------------------------------------------

def servers_dir() -> Path:
    """Return marimo's local server-registry directory."""
    if sys.platform.startswith("win"):
        return Path.home() / ".marimo" / "servers"
    state_home = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(state_home) / "marimo" / "servers"


def pid_alive(pid: int) -> bool:
    """Return True if *pid* refers to a live process."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    except OSError:
        return False
    return True


def list_servers(
    *,
    directory: Path | None = None,
    prune: bool = False,
    is_alive: Callable[[int], bool] = pid_alive,
) -> list[dict[str, Any]]:
    """Return registry entries for live marimo servers.

    Parameters
    ----------
    directory : Path | None
        Registry directory (default: :func:`servers_dir`).
    prune : bool
        Remove descriptors whose process is gone (marimo leaves them behind
        when a server is killed rather than shut down).
    """
    d = directory or servers_dir()
    entries: list[dict[str, Any]] = []
    if not d.is_dir():
        return entries

    for f in sorted(d.glob("*.json")):
        try:
            entry = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(entry, dict):
            continue
        pid = entry.get("pid")
        if not isinstance(pid, int) or not is_alive(pid):
            if prune:
                try:
                    f.unlink()
                except OSError:
                    pass
            continue
        entry["entry_path"] = str(f)
        entries.append(entry)
    return entries


def server_url(entry: dict[str, Any]) -> str:
    """Return a browsable URL for a registry *entry*.

    Wildcard bind addresses (``0.0.0.0``) are rendered as ``localhost`` —
    that is the address a human (or an SSH tunnel) actually opens.
    """
    host = str(entry.get("host") or "")
    display = "localhost" if host in _WILDCARD_HOSTS else host
    port = entry.get("port")
    base = str(entry.get("base_url") or "")
    return f"http://{display}:{port}{base}"


# ---------------------------------------------------------------------------
# Server → notebook mapping
# ---------------------------------------------------------------------------

def _norm(p: Path | str) -> Path:
    try:
        return Path(p).expanduser().resolve()
    except OSError:
        return Path(p)


def proc_argv(pid: int) -> list[str]:
    """Return the command line of *pid* (empty when unavailable)."""
    proc_file = Path("/proc") / str(pid) / "cmdline"
    try:
        raw = proc_file.read_bytes()
        return [a for a in raw.decode("utf-8", "replace").split("\0") if a]
    except OSError:
        pass
    try:
        import psutil  # type: ignore[import-not-found]

        return list(psutil.Process(pid).cmdline())
    except Exception:
        return []


def proc_cwd(pid: int) -> Path | None:
    """Return the working directory of *pid* (None when unavailable)."""
    try:
        return Path(os.readlink(f"/proc/{pid}/cwd"))
    except OSError:
        pass
    try:
        import psutil  # type: ignore[import-not-found]

        return Path(psutil.Process(pid).cwd())
    except Exception:
        return None


def notebooks_from_argv(pid: int) -> list[dict[str, str]]:
    """Notebook paths a marimo process was started with, from its argv."""
    argv = proc_argv(pid)
    if not argv:
        return []
    cwd = proc_cwd(pid)
    hits: list[dict[str, str]] = []
    for arg in argv[1:]:
        if arg.startswith("-") or not arg.endswith(".py"):
            continue
        p = Path(arg)
        if not p.is_absolute() and cwd is not None:
            p = cwd / p
        hits.append({"path": str(_norm(p)), "session_id": "", "source": "argv"})
    return hits


def notebooks_from_http(
    entry: dict[str, Any], *, timeout: float = 1.0,
) -> list[dict[str, str]]:
    """Notebook paths from a server's ``/api/sessions`` endpoint."""
    import urllib.request

    url = f"{server_url(entry)}/api/sessions"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8", "replace"))
    except Exception:
        return []
    if not isinstance(data, dict):
        return []

    hits: list[dict[str, str]] = []
    for session_id, info in data.items():
        if not isinstance(info, dict):
            continue
        path = info.get("path") or info.get("filename")
        if not path:
            continue
        hits.append({
            "path": str(_norm(path)),
            "session_id": str(session_id),
            "source": "http",
        })
    return hits


def server_notebooks(
    entry: dict[str, Any], *, http: bool = True, timeout: float = 1.0,
) -> list[dict[str, str]]:
    """Return ``[{path, session_id, source}]`` for everything *entry* serves."""
    pid = entry.get("pid")
    hits = notebooks_from_argv(pid) if isinstance(pid, int) else []
    if http:
        hits.extend(notebooks_from_http(entry, timeout=timeout))
    return hits


def find_server_for_notebook(
    py_path: Path,
    *,
    servers: Sequence[dict[str, Any]] | None = None,
    probe: Callable[[dict[str, Any]], Iterable[dict[str, str]]] | None = None,
    http: bool = True,
    http_timeout: float = 1.0,
) -> dict[str, Any] | None:
    """Return the live server already serving *py_path*, or ``None``.

    This is what keeps a second launch from orphaning the first server — a
    stale headless server owns the file and will happily save over on-disk
    edits made while it was running.

    Probing runs in two passes: argv for every server first (free), then HTTP
    only if nothing matched, so the common case costs no network round-trips
    even on a host with a dozen live servers.
    """
    target = _norm(py_path)
    entries = list(servers) if servers is not None else list_servers(prune=True)

    if probe is not None:
        passes: list[Callable[[dict[str, Any]], Iterable[dict[str, str]]]] = [probe]
    else:
        passes = [lambda e: notebooks_from_argv(e.get("pid"))
                  if isinstance(e.get("pid"), int) else []]
        if http:
            passes.append(lambda e: notebooks_from_http(e, timeout=http_timeout))

    for probe_fn in passes:
        for entry in entries:
            for hit in probe_fn(entry):
                if _norm(hit.get("path", "")) != target:
                    continue
                return {
                    "server_id": entry.get("server_id", ""),
                    "pid": entry.get("pid"),
                    "host": entry.get("host", ""),
                    "port": entry.get("port"),
                    "url": server_url(entry),
                    "session_id": hit.get("session_id", ""),
                    "matched_by": hit.get("source", ""),
                    "started_at": entry.get("started_at", ""),
                    "notebook_path": str(target),
                }
    return None


# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

def build_command(
    py_path: Path,
    *,
    marimo_cmd: Sequence[str],
    host: str = DEFAULT_HOST,
    port: int = 0,
    headless: bool = True,
    token: bool = False,
    extra_args: Sequence[str] = (),
) -> list[str]:
    """Build the ``marimo edit`` command line.

    ``--no-token`` is the default on purpose: only token-less servers register
    in the local registry, which is what makes them discoverable (and
    dedupable) by agents.  No ``--port`` is passed unless pinned, so marimo
    picks a free one and several notebooks can coexist.
    """
    cmd = [*marimo_cmd, "edit"]
    if headless:
        cmd.append("--headless")
    if not token:
        cmd.append("--no-token")
    cmd.extend(["--host", host])
    if port:
        cmd.extend(["--port", str(port)])
    cmd.append(str(py_path))
    cmd.extend(extra_args)
    return cmd


def launch(
    py_path: Path,
    *,
    marimo_cmd: Sequence[str],
    cwd: Path,
    host: str = DEFAULT_HOST,
    port: int = 0,
    headless: bool = True,
    token: bool = False,
    extra_args: Sequence[str] = (),
    log_path: Path | None = None,
    timeout: float = 25.0,
    poll: float = 0.25,
    discover: Callable[[], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Start a detached marimo server and wait for it to register.

    Returns a dict with ``status`` (``started`` / ``unregistered`` / ``error``),
    ``pid``, ``url``, ``host``, ``port``, ``server_id``, ``command`` and
    ``log``.  ``unregistered`` means the process is alive but never showed up
    in the registry within *timeout* — the log usually says why.
    """
    discover_fn = discover or (lambda: list_servers(prune=False))
    cmd = build_command(
        py_path,
        marimo_cmd=marimo_cmd,
        host=host,
        port=port,
        headless=headless,
        token=token,
        extra_args=extra_args,
    )

    before = {e.get("server_id") for e in discover_fn()}

    log_handle = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("wb")

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdin=subprocess.DEVNULL,
            stdout=log_handle or subprocess.DEVNULL,
            stderr=subprocess.STDOUT if log_handle else subprocess.DEVNULL,
            start_new_session=True,  # survives this process; not killed on exit
        )
    except FileNotFoundError:
        return {
            "status": "error",
            "error": f"marimo command not found: {' '.join(cmd[:2])}. "
                     "Install marimo in the project env (pip install marimo).",
            "command": " ".join(cmd),
        }
    finally:
        if log_handle is not None:
            log_handle.close()

    base = {
        "pid": proc.pid,
        "command": " ".join(cmd),
        "log": str(log_path) if log_path else "",
        "notebook_path": str(py_path),
    }

    deadline = time.monotonic() + timeout
    while True:
        entry = _match_entry(discover_fn(), pid=proc.pid, port=port, before=before)
        if entry is not None:
            return {
                **base,
                "status": "started",
                "server_id": entry.get("server_id", ""),
                "host": entry.get("host", host),
                "port": entry.get("port", port),
                "url": server_url(entry),
            }
        if proc.poll() is not None:
            return {
                **base,
                "status": "error",
                "exit_code": proc.returncode,
                "error": "marimo exited before registering a server",
                "log_tail": _tail(log_path),
            }
        if time.monotonic() >= deadline:
            return {
                **base,
                "status": "unregistered",
                "host": host,
                "port": port,
                "url": f"http://localhost:{port}" if port else "",
                "warning": f"server did not register within {timeout:g}s "
                           "(still starting, or started with token auth)",
                "log_tail": _tail(log_path),
            }
        time.sleep(poll)


def _match_entry(
    entries: Sequence[dict[str, Any]],
    *,
    pid: int,
    port: int,
    before: set[Any],
) -> dict[str, Any] | None:
    """Pick the registry entry belonging to a just-launched process."""
    for entry in entries:
        if entry.get("pid") == pid:
            return entry
    # A wrapper (``pixi run``/``conda run``) means the registered pid is a
    # grandchild — fall back to the pinned port, then to whatever is new.
    if port:
        for entry in entries:
            if entry.get("port") == port:
                return entry
    for entry in entries:
        if entry.get("server_id") not in before:
            return entry
    return None


def _tail(log_path: Path | None, limit: int = 1200) -> str:
    if log_path is None or not log_path.exists():
        return ""
    try:
        return log_path.read_text(encoding="utf-8", errors="replace")[-limit:]
    except OSError:
        return ""
