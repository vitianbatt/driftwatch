"""curator.py – deduplicate and retain only the most recent result per service."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class CuratorError(Exception):
    """Raised when curation fails."""


@dataclass
class CuratedReport:
    """Holds the curated (deduplicated) set of results."""

    results: List[DriftResult] = field(default_factory=list)
    dropped: int = 0

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def summary(self) -> str:
        kept = len(self.results)
        if kept == 0:
            return f"No results retained (dropped {self.dropped})."
        drifted = sum(1 for r in self.results if r.diffs)
        return (
            f"Retained {kept} result(s), dropped {self.dropped} duplicate(s). "
            f"{drifted} service(s) have drift."
        )


def curate(results: Optional[List[DriftResult]]) -> CuratedReport:
    """Keep only the last-seen result for each service name.

    Args:
        results: List of DriftResult objects, possibly containing duplicates.

    Returns:
        A CuratedReport with one result per service (last wins).

    Raises:
        CuratorError: If *results* is None.
    """
    if results is None:
        raise CuratorError("results must not be None")

    seen: Dict[str, DriftResult] = {}
    for r in results:
        seen[r.service] = r

    dropped = len(results) - len(seen)
    return CuratedReport(results=list(seen.values()), dropped=dropped)
