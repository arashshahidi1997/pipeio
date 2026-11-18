"""Registry-family path construction shared by ``BidsPaths`` and ``SimpleResolver``.

A flow's Snakefile builds its outputs with ``BidsPaths(registry, output_dir,
inputs)``, which merges a registry family into keyword arguments and hands
them to ``snakebids.bids()``.  Agent tools (``pipeio_target_paths``,
``PipelineContext``) must return the same paths, but run in an environment
without snakebids.  This module holds both halves so they cannot drift:

- :func:`family_kwargs` merges base-input wildcards, wildcard sets, the
  family ``bids`` dict, the member dict and caller entities (used by
  ``BidsPaths`` and ``SimpleResolver`` alike).
- :func:`bids_path` reproduces ``snakebids.bids()`` for the builtin specs
  (``v0_0_0``, ``v0_11_0``, ``v0_15_0``/``latest``): ``root`` first, then
  ``sub-``/``ses-`` directories, then ``datatype``, then the filename.
  ``tests/test_bidspath.py`` checks it against snakebids when installed.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping

# (entity, tag, dir) per builtin snakebids spec; "*" marks where custom
# entities go (v0_0_0 has no marker: custom entities go after the spec ones).
_V0_0_0: tuple[tuple[str, str, bool], ...] = (
    ("subject", "sub", True),
    ("session", "ses", True),
    *((e, e, False) for e in (
        "task", "acq", "ce", "rec", "dir", "run", "mod", "echo",
        "hemi", "space", "res", "den", "label", "desc",
    )),
)

_V0_11_TAIL = (
    ("echo", "echo"), ("flip", "flip"), ("inversion", "inv"), ("mt", "mt"),
    ("processed", "proc"), ("part", "part"), ("space", "space"),
    ("atlas", "atlas"), ("segmentation", "seg"), ("hemisphere", "hemi"),
    ("resolution", "res"), ("density", "den"), ("roi", "roi"),
    ("from", "from"), ("to", "to"), ("split", "split"),
    ("recording", "recording"), ("chunk", "chunk"), ("model", "model"),
    ("subset", "subset"), ("label", "label"), ("*", "*"),
    ("description", "desc"),
)


def _spec(head: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str, bool], ...]:
    rows = [("subject", "sub", True), ("session", "ses", True)]
    rows += [(e, t, False) for e, t in head + _V0_11_TAIL]
    return tuple(rows)


_V0_11_0 = _spec((
    ("sample", "sample"), ("task", "task"), ("tracksys", "tracksys"),
    ("acquisition", "acq"), ("ceagent", "ce"), ("staining", "stain"),
    ("tracer", "trc"), ("reconstruction", "rec"), ("direction", "dir"),
    ("run", "run"), ("modality", "mod"),
))

_V0_15_0 = _spec((
    ("sample", "sample"), ("task", "task"), ("tracksys", "tracksys"),
    ("acquisition", "acq"), ("nucleus", "nuc"), ("volume", "voi"),
    ("ceagent", "ce"), ("staining", "stain"), ("tracer", "trc"),
    ("reconstruction", "rec"), ("direction", "dir"), ("run", "run"),
    ("modality", "mod"),
))

SPECS: dict[str, tuple[tuple[str, str, bool], ...]] = {
    "v0_0_0": _V0_0_0,
    "v0_11_0": _V0_11_0,
    "v0_15_0": _V0_15_0,
    "latest": _V0_15_0,
}

_SET_SPEC_RE = re.compile(r"""set_bids_spec\(\s*["'](\w+)["']\s*\)""")


def detect_bids_spec(*texts: str) -> str:
    """Return the spec named by ``set_bids_spec("...")`` in *texts*, else ``latest``.

    snakebids uses its latest spec when a workflow never calls
    ``set_bids_spec``, so that is the default here too.
    """
    for text in texts:
        m = _SET_SPEC_RE.search(text or "")
        if m and m.group(1) in SPECS:
            return m.group(1)
    return "latest"


def flow_bids_spec(flow_dir: Path) -> str:
    """Detect the bids spec a flow's Snakefile (and ``rules/*.smk``) sets."""
    texts: list[str] = []
    for f in [flow_dir / "Snakefile", *sorted((flow_dir / "rules").glob("*.smk"))]:
        try:
            texts.append(f.read_text(encoding="utf-8"))
        except OSError:
            continue
    return detect_bids_spec(*texts)


def entity_tags(spec: str = "latest") -> dict[str, str]:
    """Map every entity name and tag of *spec* to its filename tag."""
    out: dict[str, str] = {}
    for entity, tag, _ in SPECS[spec]:
        if entity != "*":
            out[entity] = tag
            out[tag] = tag
    return out


def bids_path(
    root: str | Path | None = None,
    *,
    spec: str = "latest",
    datatype: str | None = None,
    prefix: str | None = None,
    suffix: str | None = None,
    extension: str | None = None,
    **entities: Any,
) -> str:
    """Build a path exactly as ``snakebids.bids()`` does under *spec*."""
    rows = SPECS[spec]
    order = [tag for _, tag, _ in rows]
    dirs = {tag for _, tag, d in rows if d}
    aliases = {entity: tag for entity, tag, _ in rows}

    parsed: dict[str, str] = {}
    for key, val in entities.items():
        stripped = key.rstrip("_")
        tag = aliases.get(stripped, stripped)
        if tag in parsed:
            raise ValueError(
                f"Long and short names of an entity cannot both be given: {key!r}"
            )
        parsed[tag] = str(val)

    spec_parts: list[str] = []
    custom_parts: list[str] = []
    split = len(order) + 1
    path_parts: list[str] = [str(root)] if root else []
    if prefix:
        spec_parts.append(prefix)
    for tag in order:
        if tag == "*":
            split = len(spec_parts)
        elif value := parsed.pop(tag, None):
            spec_parts.append(f"{tag}-{value}")
            if tag in dirs:
                path_parts.append(f"{tag}-{value}")
    custom_parts.extend(f"{k}-{v}" for k, v in parsed.items())
    if datatype:
        path_parts.append(datatype)
    tail = [suffix] if suffix is not None else []
    path_parts.append(
        "_".join([*spec_parts[:split], *custom_parts, *spec_parts[split:], *tail])
    )
    result = os.path.join(*path_parts)
    if extension is not None:
        result += extension
    return result


def family_kwargs(
    registry: Mapping[str, Any],
    family: str,
    member: str | None = None,
    entities: Mapping[str, Any] | None = None,
    *,
    base_wildcards: Mapping[str, Mapping[str, str]] | None = None,
    wildcard_sets: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    """Merge a registry family into ``bids()`` keyword arguments (``root`` included).

    Precedence, lowest first: base-input wildcards, the named wildcard set,
    the family ``bids`` dict, the member dict, *entities*.  YAML nulls are
    dropped.  Raises ``KeyError`` for an unknown family or member and
    ``ValueError`` when a family needs inputs or wildcard sets that were not
    given.
    """
    if family not in registry:
        opts = ", ".join(sorted(registry)[:30])
        more = " ..." if len(registry) > 30 else ""
        raise KeyError(f"Unknown family '{family}'. Available: {opts}{more}")
    fam = registry[family]
    members = fam.get("members") or {}
    if member is None:
        member = fam.get("default_member") or next(iter(members))
    if member not in members:
        opts = ", ".join(sorted(members))
        raise KeyError(
            f"Unknown member '{member}' for family '{family}'. Available: {opts}"
        )

    kwargs: dict[str, Any] = {}
    base = fam.get("base_input")
    if base:
        if base_wildcards is None:
            raise ValueError(
                f"Family '{family}' specifies base_input='{base}', "
                "but no inputs were provided to BidsPaths."
            )
        if base not in base_wildcards:
            raise KeyError(
                f"Family '{family}' specifies base_input='{base}', "
                "but no such input found in registry."
            )
        kwargs.update(base_wildcards[base])

    wildcard_set = fam.get("wildcards", [])
    if wildcard_set:
        if not wildcard_sets:
            raise ValueError(
                f"Family '{family}' specifies wildcards='{wildcard_set}', "
                "but no wildcard sets were provided to BidsPaths."
            )
        if wildcard_set not in wildcard_sets:
            raise KeyError(
                f"Family '{family}' specifies wildcards='{wildcard_set}', "
                "but no such wildcard set found in registry."
            )
        kwargs.update(wildcard_sets[wildcard_set])

    kwargs.update(fam.get("bids") or {})
    kwargs.update(members[member] or {})
    kwargs.update(entities or {})
    return {k: v for k, v in kwargs.items() if v is not None}


def config_base_wildcards(config: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """Wildcard templates per input, read from a flow config without scanning.

    ``snakebids.generate_inputs`` gives each input ``wildcards`` of the form
    ``{"subject": "{subject}", ...}`` from its ``pybids_inputs`` entry.  This
    reads them from every ``pybids_inputs*`` mapping in the config, so path
    templates can be built without touching the dataset.
    """
    out: dict[str, dict[str, str]] = {}
    for key, block in config.items():
        if not (isinstance(key, str) and key.startswith("pybids_inputs")):
            continue
        if not isinstance(block, Mapping):
            continue
        for name, spec in block.items():
            if isinstance(spec, Mapping):
                wcs = spec.get("wildcards") or []
                out[str(name)] = {str(w): f"{{{w}}}" for w in wcs}
    return out


def align_entities(
    entities: Mapping[str, Any], known: Mapping[str, Any], spec: str = "latest"
) -> dict[str, Any]:
    """Rename caller entities to the spelling already used in *known* keys.

    A base input may name a wildcard ``subject`` while the caller passes
    ``sub`` (or ``acquisition`` vs ``acq``); both would reach ``bids()`` and
    collide.  Keys that share a tag (under *spec* or the common BIDS
    long/short pairs) are renamed to the *known* spelling.
    """
    tags = {**_COMMON_TAGS, **entity_tags(spec)}

    def tag(k: str) -> str:
        return tags.get(k, k)

    by_tag = {tag(k): k for k in known}
    return {by_tag.get(tag(k), k): v for k, v in entities.items()}


_COMMON_TAGS = {
    "subject": "sub", "sub": "sub", "session": "ses", "ses": "ses",
    "acquisition": "acq", "acq": "acq", "description": "desc", "desc": "desc",
    "reconstruction": "rec", "rec": "rec", "hemisphere": "hemi", "hemi": "hemi",
}
