"""BIDS/snakebids adapter for PathResolver.

Requires the ``bids`` extra: ``pip install pipeio[bids]``
"""

from __future__ import annotations

from pathlib import Path


class BidsResolver:
    """PathResolver implementation backed by snakebids BidsPaths.

    This adapter translates pipeio's generic (group, member, entities)
    interface into snakebids path template expansion.

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

    def resolve(self, group: str, member: str, **entities: str) -> Path:
        """Resolve a single BIDS-compliant path."""
        raise NotImplementedError("BidsResolver.resolve — see specs for design")

    def expand(self, group: str, member: str, **filters: str) -> list[Path]:
        """Expand all matching BIDS paths."""
        raise NotImplementedError("BidsResolver.expand — see specs for design")
