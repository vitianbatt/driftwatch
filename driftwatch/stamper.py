"""stamper.py — attaches version stamps to drift results for traceability."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class StamperError(Exception):
    """Raised when stamping fails."""


@dataclass
class StampedResult:
    service: str
    drifted_fields: List[str]
    stamp: str  # e.g. a version string, git SHA, or run ID
    source: Optional[str] = None

    def has_drift(self) -> bool:
        return bool(self.drifted_fields)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "drifted_fields": list(self.drifted_fields),
            "stamp": self.stamp,
            "source": self.source,
        }


@dataclass
class StampReport:
    results: List[StampedResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def summary(self) -> str:
        total = len(self.results)
        drifted = sum(1 for r in self.results if r.has_drift())
        return f"{drifted}/{total} services drifted (stamp applied)"


def stamp_results(
    results: List[DriftResult],
    stamp: str,
    source: Optional[str] = None,
) -> StampReport:
    """Attach *stamp* to every DriftResult and return a StampReport."""
    if results is None:
        raise StamperError("results must not be None")
    if not stamp or not stamp.strip():
        raise StamperError("stamp must be a non-empty string")

    stamped = [
        StampedResult(
            service=r.service,
            drifted_fields=list(r.drifted_fields),
            stamp=stamp,
            source=source,
        )
        for r in results
    ]
    return StampReport(results=stamped)
