"""BIDS/snakebids adapter for PathResolver.

Translates pipeio's generic (group, member, entities) interface into
BIDS-compliant path templates via snakebids.

Requires the ``bids`` extra: ``pip install pipeio[bids]``
"""

from __future__ import annotations

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
