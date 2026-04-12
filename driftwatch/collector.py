"""collector.py – aggregates raw drift results into a CollectedReport.

A CollectedReport holds a named batch of DriftResult objects and provides
convenience accessors used by downstream pipeline stages.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class CollectorError(Exception):
    """Raised when collection fails."""


@dataclass
class CollectedReport:
    name: str
    results: List[DriftResult] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise CollectorError("CollectedReport name must not be empty")
        if self.results is None:
            raise CollectorError("results must not be None")

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        """Return service names in insertion order."""
        return [r.service for r in self.results]

    def drifted(self) -> List[DriftResult]:
        """Return only results that have drift."""
        return [r for r in self.results if r.diffs]

    def clean(self) -> List[DriftResult]:
        """Return only results with no drift."""
        return [r for r in self.results if not r.diffs]

    def has_any_drift(self) -> bool:
        return bool(self.drifted())

    def summary(self) -> str:
        total = len(self.results)
        n_drift = len(self.drifted())
        if total == 0:
            return f"{self.name}: no results collected"
        if n_drift == 0:
            return f"{self.name}: all {total} service(s) clean"
        return f"{self.name}: {n_drift}/{total} service(s) drifted"


def collect(name: str, results: Optional[List[DriftResult]]) -> CollectedReport:
    """Validate and wrap *results* in a CollectedReport named *name*."""
    if results is None:
        raise CollectorError("Cannot collect None results")
    return CollectedReport(name=name, results=list(results))
