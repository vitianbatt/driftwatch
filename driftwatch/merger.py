"""Merge multiple DriftResult lists into a unified view, deduplicating by service name."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict, Optional

from driftwatch.comparator import DriftResult


class MergerError(Exception):
    """Raised when merging fails due to invalid input."""


@dataclass
class MergedReport:
    """Holds the result of merging several DriftResult collections."""

    results: List[DriftResult] = field(default_factory=list)
    source_count: int = 0
    conflict_services: List[str] = field(default_factory=list)

    def has_conflicts(self) -> bool:
        return len(self.conflict_services) > 0

    def summary(self) -> str:
        total = len(self.results)
        drifted = sum(1 for r in self.results if r.diffs)
        lines = [
            f"Merged {total} service(s) from {self.source_count} source(s).",
            f"Drifted: {drifted}, Clean: {total - drifted}.",
        ]
        if self.conflict_services:
            joined = ", ".join(self.conflict_services)
            lines.append(f"Conflicts resolved (last-write-wins) for: {joined}.")
        return "\n".join(lines)


def _result_key(result: DriftResult) -> str:
    return result.service


def merge_results(
    sources: List[List[DriftResult]],
    *,
    strategy: str = "last",
) -> MergedReport:
    """Merge multiple lists of DriftResult into one.

    Args:
        sources: A list of DriftResult lists to merge.
        strategy: Conflict resolution strategy. Only ``"last"`` is supported;
                  when the same service appears in multiple sources the entry
                  from the latest source wins.

    Returns:
        A :class:`MergedReport` describing the merged state.

    Raises:
        MergerError: If *sources* is None or *strategy* is unsupported.
    """
    if sources is None:
        raise MergerError("sources must not be None")
    if strategy != "last":
        raise MergerError(f"Unsupported merge strategy: {strategy!r}")

    seen: Dict[str, DriftResult] = {}
    conflicts: List[str] = []

    for batch in sources:
        if batch is None:
            raise MergerError("Individual source list must not be None")
        for result in batch:
            key = _result_key(result)
            if key in seen:
                if key not in conflicts:
                    conflicts.append(key)
            seen[key] = result

    return MergedReport(
        results=list(seen.values()),
        source_count=len(sources),
        conflict_services=conflicts,
    )
