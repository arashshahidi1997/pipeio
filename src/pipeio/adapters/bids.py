"""BIDS/snakebids adapter for PathResolver.

Translates pipeio's generic (group, member, entities) interface into
BIDS-compliant path templates via snakebids.

Requires the ``bids`` extra: ``pip install pipeio[bids]``
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class BidsResolver:
    """PathResolver implementation backed by snakebids-style BIDS path templates.

    Reads BIDS path configuration from a flow's ``config.yml``, which must
    contain a ``registry`` section mapping groups to BIDS parameters::

        registry:
          deriv_preproc:
            bids:
              root: derivatives/preproc
              datatype: ieeg
            members:
              cleaned:
                suffix: cleaned
                extension: .nii.gz

    Path construction follows BIDS conventions::

        {output_dir}/{bids.root}/sub-{sub}/[ses-{ses}/][{datatype}/]
            sub-{sub}[_ses-{ses}]..._suffix-{suffix}{extension}

    Requires: ``pip install pipeio[bids]``
    """

    def __init__(self, config_path: Path) -> None:
        try:
            import snakebids  # noqa: F401
        except ImportError:
            raise ImportError(
                "BidsResolver requires snakebids. "
                "Install with: pip install pipeio[bids]"
            )
        self._config_path = config_path
        self._config = self._load_config(config_path)

    @staticmethod
    def _load_config(config_path: Path) -> dict[str, Any]:
        with open(config_path) as fh:
            return yaml.safe_load(fh) or {}

    def _group_meta(self, group: str) -> dict[str, Any]:
        registry = self._config.get("registry", {})
        grp = registry.get(group)
        if grp is None:
            raise KeyError(f"Unknown group: {group!r}")
        return grp

    def _member_meta(self, group_meta: dict, member: str) -> dict[str, Any]:
        members = group_meta.get("members", {})
        mem = members.get(member)
        if mem is None:
            known = ", ".join(members.keys()) or "(none)"
            raise KeyError(f"Unknown member: {member!r}. Known: {known}")
        return mem

    def resolve(self, group: str, member: str, **entities: str) -> Path:
        """Resolve a single BIDS-compliant path.

        Constructs::

            {output_dir}/{bids_root}/sub-{sub}/[ses-{ses}/][{datatype}/]
                sub-{sub}[_ses-{ses}]..._suffix-{suffix}{extension}
        """
        grp = self._group_meta(group)
        mem = self._member_meta(grp, member)
        bids = grp.get("bids", {})

        output_dir = self._config.get("output_dir", "")
        bids_root = bids.get("root", group)
        datatype = bids.get("datatype", "")

        # Build directory: sub-XX/[ses-YY/][datatype/]
        dir_parts = [output_dir, bids_root]
        if "sub" in entities:
            dir_parts.append(f"sub-{entities['sub']}")
        if "ses" in entities:
            dir_parts.append(f"ses-{entities['ses']}")
        if datatype:
            dir_parts.append(datatype)

        # Build filename: sub-XX[_ses-YY][_other-ZZ]_suffix-{suffix}{ext}
        # BIDS entity ordering: sub, ses, then alphabetical
        ordered_keys = []
        if "sub" in entities:
            ordered_keys.append("sub")
        if "ses" in entities:
            ordered_keys.append("ses")
        for k in sorted(entities):
            if k not in ("sub", "ses"):
                ordered_keys.append(k)

        name_parts = [f"{k}-{entities[k]}" for k in ordered_keys]
        suffix = mem.get("suffix", member)
        extension = mem.get("extension", "")
        name_parts.append(f"suffix-{suffix}")
        filename = "_".join(name_parts) + extension

        return Path("/".join(p for p in dir_parts if p)) / filename

    def expand(self, group: str, member: str, **filters: str) -> list[Path]:
        """Expand all matching BIDS paths by globbing the filesystem.

        Globs the BIDS directory tree for files matching the member's
        suffix/extension pattern, then filters by entity key-value pairs.
        """
        grp = self._group_meta(group)
        mem = self._member_meta(grp, member)
        bids = grp.get("bids", {})

        output_dir = self._config.get("output_dir", "")
        bids_root = bids.get("root", group)
        suffix = mem.get("suffix", member)
        extension = mem.get("extension", "")

        base = Path(output_dir) / bids_root
        if not base.exists():
            return []

        pattern = f"**/*suffix-{suffix}{extension}"
        matches = sorted(base.glob(pattern))

        if filters:
            filtered = []
            for p in matches:
                path_str = str(p)
                if all(f"{k}-{v}" in path_str for k, v in filters.items()):
                    filtered.append(p)
            matches = filtered

        return matches


# ---------------------------------------------------------------------------
# BidsPaths — snakebids-backed path resolver (ported from sutil.bids.paths)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FamilyView(Mapping[str, str]):
    """Dict-like view over the members of a single registry family."""

    _paths: BidsPaths
    _family: str

    def __getitem__(self, member: str) -> str:
        return self._paths(self._family, member)

    def __iter__(self) -> Iterator[str]:
        return iter(self._paths.members(self._family))

    def __len__(self) -> int:
        return len(self._paths.members(self._family))

    def path(self, member: str, **entities: Any) -> str:
        return self._paths(self._family, member, **entities)

    def __repr__(self) -> str:
        mem = self._paths.members(self._family)
        preview = ", ".join(mem[:6]) + (" ..." if len(mem) > 6 else "")
        return f"<family {self._family}: {preview}>"


class BidsPaths(Mapping[str, _FamilyView]):
    """BIDS path resolver using ``snakebids.bids()`` for path construction.

    Drop-in replacement for the former ``sutil.bids.paths.BidsPaths``.
    Requires ``snakebids`` (``pip install pipeio[bids]``).

    Parameters
    ----------
    registry : dict
        Registry mapping families → ``{bids: {…}, members: {…}, …}``.
    root_dir : str | Path
        Root output directory for constructed paths.
    base_inputs : dict | None
        Output of ``snakebids.generate_inputs()`` — provides wildcard
        templates inherited via ``base_input`` in registry families.
    wildcard_sets : dict | None
        Named wildcard sets referenced by ``wildcards`` in registry families.

    Usage
    -----
    ::

        from snakebids import generate_inputs
        from pipeio.adapters.bids import BidsPaths

        inputs = generate_inputs(bids_dir, config["pybids_inputs"])
        out = BidsPaths(config["registry"], output_dir, inputs)
        path = out("badlabel", "npy", subject="test", session="01")
    """

    def __init__(
        self,
        registry: dict,
        root_dir: str | Path,
        base_inputs: dict[str, Any] | None = None,
        wildcard_sets: dict[str, Any] | None = None,
    ) -> None:
        try:
            from snakebids import bids as _bids  # noqa: F401
        except ImportError:
            raise ImportError(
                "BidsPaths requires snakebids. "
                "Install with: pip install pipeio[bids]"
            )
        self.registry = registry
        self.root_dir = Path(root_dir)
        self.base_inputs = base_inputs
        self.wildcard_sets = wildcard_sets

    # ---- Mapping interface ----

    def __getitem__(self, family: str) -> _FamilyView:
        if family not in self.registry:
            raise KeyError(self._unknown_family(family))
        return _FamilyView(self, family)

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(self.registry.keys()))

    def __len__(self) -> int:
        return len(self.registry)

    # ---- Introspection ----

    def members(self, family: str) -> list[str]:
        """Return sorted member names for *family*."""
        if family not in self.registry:
            raise KeyError(self._unknown_family(family))
        return sorted(self.registry[family]["members"].keys())

    def artifacts(self) -> str:
        """Return a human-readable listing of all families and members."""
        lines: list[str] = []
        for fam in sorted(self.registry):
            lines.append(f"{fam}:")
            for mem in self.members(fam):
                lines.append(f"  - {mem}")
        return "\n".join(lines)

    def _unknown_family(self, family: str) -> str:
        opts = ", ".join(sorted(self.registry)[:30])
        more = " ..." if len(self.registry) > 30 else ""
        return f"Unknown family '{family}'. Available: {opts}{more}"

    def _unknown_member(self, family: str, member: str) -> str:
        opts = ", ".join(self.members(family))
        return f"Unknown member '{member}' for family '{family}'. Available: {opts}"

    # ---- Core path builder ----

    def __call__(
        self, family: str, member: str | None = None, **entities: Any
    ) -> str:
        """Resolve a BIDS path string for *(family, member)* with entity overrides.

        Delegates to ``snakebids.bids()`` after merging registry defaults,
        base_input wildcards, and caller-supplied entity overrides.
        """
        from snakebids import bids

        if family not in self.registry:
            raise KeyError(self._unknown_family(family))

        fam = self.registry[family]

        if member is None:
            member = fam.get("default_member") or next(iter(fam["members"]))

        if member not in fam["members"]:
            raise KeyError(self._unknown_member(family, member))

        mem = fam["members"][member]

        kwargs: dict[str, Any] = {}

        # Inherit wildcard templates from base_input
        base = fam.get("base_input")
        if base:
            if not self.base_inputs:
                raise ValueError(
                    f"Family '{family}' specifies base_input='{base}', "
                    "but no inputs were provided to BidsPaths."
                )
            if base not in self.base_inputs:
                raise KeyError(
                    f"Family '{family}' specifies base_input='{base}', "
                    "but no such input found in registry."
                )
            kwargs.update(self.base_inputs[base].wildcards)

        # Named wildcard sets
        wildcard_set = fam.get("wildcards", [])
        if wildcard_set:
            if not self.wildcard_sets:
                raise ValueError(
                    f"Family '{family}' specifies wildcards='{wildcard_set}', "
                    "but no wildcard sets were provided to BidsPaths."
                )
            if wildcard_set not in self.wildcard_sets:
                raise KeyError(
                    f"Family '{family}' specifies wildcards='{wildcard_set}', "
                    "but no such wildcard set found in registry."
                )
            kwargs.update(self.wildcard_sets[wildcard_set])

        # Family-level BIDS defaults
        kwargs.update(fam.get("bids", {}))

        # Member-level overrides
        kwargs.update(mem)

        # Caller entity overrides
        kwargs.update(entities)

        # Root handling
        root_rel = kwargs.pop("root", "")
        root = self.root_dir / root_rel if root_rel else self.root_dir

        # Drop YAML nulls
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        return bids(root=root, **kwargs)

    def __repr__(self) -> str:
        n = len(self.registry)
        return f"<BidsPaths: {n} families, root={self.root_dir}>"
