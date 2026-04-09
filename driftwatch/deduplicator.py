"""Deduplicator: removes duplicate DriftResult entries across multiple runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict, Tuple

from driftwatch.comparator import DriftResult


class DeduplicatorError(Exception):
    """Raised when deduplication encounters invalid input."""


@dataclass
class DeduplicatedReport:
    unique: List[DriftResult] = field(default_factory=list)
    duplicate_count: int = 0

    def total_seen(self) -> int:
        return len(self.unique) + self.duplicate_count

    def summary(self) -> str:
        if not self.unique:
            return "No results after deduplication."
        lines = [f"Unique services: {len(self.unique)}, duplicates dropped: {self.duplicate_count}"]
        for r in self.unique:
            status = "DRIFT" if r.has_drift() else "OK"
            lines.append(f"  [{status}] {r.service}")
        return "\n".join(lines)


def _result_key(result: DriftResult) -> Tuple[str, frozenset]:
    """Build a hashable key from service name and drifted field names."""
    return (result.service, frozenset(result.diffs.keys() if result.diffs else []))


def deduplicate(results: List[DriftResult]) -> DeduplicatedReport:
    """Return a DeduplicatedReport keeping only the first occurrence of each unique result."""
    if results is None:
        raise DeduplicatorError("results must not be None")

    seen: Dict[Tuple, bool] = {}
    unique: List[DriftResult] = []
    duplicate_count = 0

    for r in results:
        if not isinstance(r, DriftResult):
            raise DeduplicatorError(f"Expected DriftResult, got {type(r).__name__}")
        key = _result_key(r)
        if key in seen:
            duplicate_count += 1
        else:
            seen[key] = True
            unique.append(r)

    return DeduplicatedReport(unique=unique, duplicate_count=duplicate_count)
