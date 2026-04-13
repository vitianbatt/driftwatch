"""trender.py – tracks drift counts over time and reports trends."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from driftwatch.comparator import DriftResult


class TrenderError(Exception):
    """Raised when trend analysis fails."""


@dataclass
class TrendPoint:
    """A single observation in the drift time-series for one service."""

    service: str
    drift_count: int
    timestamp: str  # ISO-8601 string

    def to_dict(self) -> Dict:
        return {
            "service": self.service,
            "drift_count": self.drift_count,
            "timestamp": self.timestamp,
        }


@dataclass
class TrendReport:
    """Aggregated trend data across multiple runs."""

    points: List[TrendPoint] = field(default_factory=list)

    def services(self) -> List[str]:
        """Return sorted unique service names present in the report."""
        return sorted({p.service for p in self.points})

    def points_for(self, service: str) -> List[TrendPoint]:
        """Return all trend points for a given service, in insertion order."""
        return [p for p in self.points if p.service == service]

    def is_increasing(self, service: str) -> bool:
        """Return True if the last observed drift count is higher than the first."""
        pts = self.points_for(service)
        if len(pts) < 2:
            return False
        return pts[-1].drift_count > pts[0].drift_count

    def is_decreasing(self, service: str) -> bool:
        """Return True if the last observed drift count is lower than the first."""
        pts = self.points_for(service)
        if len(pts) < 2:
            return False
        return pts[-1].drift_count < pts[0].drift_count

    def summary(self) -> str:
        lines = []
        for svc in self.services():
            pts = self.points_for(svc)
            direction = (
                "increasing" if self.is_increasing(svc)
                else "decreasing" if self.is_decreasing(svc)
                else "stable"
            )
            lines.append(f"{svc}: {len(pts)} point(s), trend={direction}")
        return "\n".join(lines) if lines else "no trend data"


def build_trend(results_over_time: List[List[DriftResult]], timestamps: List[str]) -> TrendReport:
    """Build a TrendReport from multiple snapshots of DriftResult lists.

    Args:
        results_over_time: Ordered list of result batches (oldest first).
        timestamps: ISO timestamp string for each batch; must match length.

    Returns:
        TrendReport populated with TrendPoints.

    Raises:
        TrenderError: If inputs are None or lengths mismatch.
    """
    if results_over_time is None or timestamps is None:
        raise TrenderError("results_over_time and timestamps must not be None")
    if len(results_over_time) != len(timestamps):
        raise TrenderError(
            f"results_over_time length ({len(results_over_time)}) "
            f"does not match timestamps length ({len(timestamps)})"
        )
    report = TrendReport()
    for batch, ts in zip(results_over_time, timestamps):
        for result in batch:
            drift_count = len(result.diffs) if result.diffs else 0
            report.points.append(
                TrendPoint(service=result.service, drift_count=drift_count, timestamp=ts)
            )
    return report
