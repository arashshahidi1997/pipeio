"""Read the registry a flow's Snakefile builds at runtime.

Snakefiles often extend ``config["registry"]`` in Python (one family per
band, per detection, ...) before building ``BidsPaths``.  Those families
exist only once the Snakefile runs, so tools that read ``config.yml`` cannot
see them.  :func:`snakefile_registry` loads the Snakefile with the snakemake
API in the project environment and dumps every ``BidsPaths`` it defines:
the registry, the root directory, the base-input wildcards and the active
snakebids spec.

``snakebids.generate_inputs`` is replaced during the load by a stub that
returns empty components built from the ``pybids_inputs`` it is given, so
the dataset is never scanned (a full scan can take minutes); only wildcard
names matter for path templates.  The result is cached under
``.projio/pipeio/cache/registry/<flow>.json`` until a flow file changes.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
from pathlib import Path
from typing import Any

_HELPER = r'''
import json, sys
from pathlib import Path

import snakebids
from snakebids import BidsComponent, bids

def _stub_generate_inputs(bids_dir, pybids_inputs, *args, **kwargs):
    out = {}
    for name, spec in (pybids_inputs or {}).items():
        wcs = [str(w) for w in (spec.get("wildcards") or [])]
        flt = spec.get("filters") or {}
        one = lambda v: v if isinstance(v, str) else None
        path = bids(
            root=str(bids_dir), datatype=one(flt.get("datatype")),
            suffix=one(flt.get("suffix")), extension=one(flt.get("extension")),
            **{w: "{" + w + "}" for w in wcs},
        )
        out[name] = BidsComponent(name=name, path=path, zip_lists={w: [] for w in wcs})
    return out

snakebids.generate_inputs = _stub_generate_inputs

from snakemake.api import SnakemakeApi
from snakemake.settings.types import OutputSettings, ResourceSettings

flow_dir = Path(sys.argv[1]).resolve()
with SnakemakeApi(OutputSettings()) as api:
    wf = api.workflow(
        resource_settings=ResourceSettings(),
        snakefile=flow_dir / "Snakefile", workdir=flow_dir,
    )
    g = wf._workflow.globals

def _spec_name():
    try:
        from snakebids.paths import specs
        from snakebids.paths._config import get_bids_spec
        active = get_bids_spec()
        for name in ("v0_0_0", "v0_11_0", "v0_15_0"):
            if getattr(specs, name)() == active:
                return name
    except Exception:
        pass
    return "latest"

paths = {}
for key, val in list(g.items()):
    if type(val).__name__ != "BidsPaths" or key.startswith("_"):
        continue
    base = {}
    for name in (val.base_inputs or {}):
        try:
            base[name] = dict(val.base_inputs[name].wildcards)
        except Exception:
            pass
    paths[key] = {
        "root_dir": str(val.root_dir),
        "registry": val.registry,
        "base_wildcards": base,
        "wildcard_sets": val.wildcard_sets,
    }
print("<<<PIPEIO-JSON>>>" + json.dumps({"spec": _spec_name(), "paths": paths}, default=str))
'''

_MARK = "<<<PIPEIO-JSON>>>"


def _flow_key(flow_dir: Path) -> str:
    """Fingerprint of the flow files that shape the registry."""
    files = [flow_dir / "Snakefile", flow_dir / "config.yml",
             *sorted((flow_dir / "rules").glob("*.smk"))]
    h = hashlib.sha1()
    for f in files:
        try:
            st = f.stat()
        except OSError:
            continue
        h.update(f"{f.name}:{st.st_mtime_ns}:{st.st_size};".encode())
    return h.hexdigest()


def cache_path(root: Path, flow: str) -> Path:
    return root / ".projio" / "pipeio" / "cache" / "registry" / f"{flow}.json"


def snakefile_registry(
    root: Path,
    flow: str,
    flow_dir: Path,
    *,
    python_cmd: str | list[str] = "",
    timeout: int = 300,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Return ``{"spec", "paths": {var: {root_dir, registry, base_wildcards, ...}}}``.

    *python_cmd* is the project Python (``"pixi run python"``); it needs
    snakemake, snakebids and the Snakefile's imports.  Raises
    ``RuntimeError`` when the Snakefile cannot be loaded.
    """
    key = _flow_key(flow_dir)
    cache = cache_path(root, flow)
    if use_cache and cache.is_file():
        try:
            data = json.loads(cache.read_text(encoding="utf-8"))
            if data.get("key") == key:
                return data["result"]
        except (OSError, ValueError, KeyError):
            pass

    prefix = (list(python_cmd) if isinstance(python_cmd, (list, tuple))
              else shlex.split(python_cmd or "python"))
    cmd = [*prefix, "-c", _HELPER, str(flow_dir)]
    try:
        proc = subprocess.run(
            cmd, cwd=root, capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Loading the Snakefile timed out after {timeout}s") from exc
    except OSError as exc:
        raise RuntimeError(f"Could not run {prefix!r}: {exc}") from exc

    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith(_MARK)), None)
    if proc.returncode != 0 or line is None:
        tail = "\n".join((proc.stderr or proc.stdout).strip().splitlines()[-15:])
        raise RuntimeError(f"Loading the Snakefile failed (exit {proc.returncode}):\n{tail}")
    result = json.loads(line[len(_MARK):])

    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps({"key": key, "result": result}), encoding="utf-8")
    except OSError:
        pass
    return result


def pick_paths(result: dict[str, Any], output_dir: Path) -> dict[str, Any] | None:
    """Choose the ``BidsPaths`` whose root is the flow's output directory.

    Falls back to the one with the most families when none matches.
    """
    paths = result.get("paths") or {}
    if not paths:
        return None
    out = output_dir.resolve()
    for info in paths.values():
        try:
            if Path(info["root_dir"]).resolve() == out:
                return info
        except OSError:
            continue
    return max(paths.values(), key=lambda i: len(i.get("registry") or {}))
