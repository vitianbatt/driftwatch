"""tracer.py — tracks drift field occurrences across multiple runs to identify persistent vs transient drift."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class TracerError(Exception):
    """Raised when tracing operations fail."""


@dataclass
class FieldTrace:
    """Tracks how many times a specific field has drifted."""
    service: str
    field_name: str
    occurrences: int = 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "field_name": self.field_name,
            "occurrences": self.occurrences,
        }


@dataclass
class TraceReport:
    """Aggregated trace data across all results."""
    traces: List[FieldTrace] = field(default_factory=list)

    def persistent(self, min_occurrences: int = 2) -> List[FieldTrace]:
        """Return traces that appear at least min_occurrences times."""
        if min_occurrences < 1:
            raise TracerError("min_occurrences must be >= 1")
        return [t for t in self.traces if t.occurrences >= min_occurrences]

    def transient(self, min_occurrences: int = 2) -> List[FieldTrace]:
        """Return traces that appear fewer than min_occurrences times."""
        if min_occurrences < 1:
            raise TracerError("min_occurrences must be >= 1")
        return [t for t in self.traces if t.occurrences < min_occurrences]

    def summary(self) -> str:
        total = len(self.traces)
        persistent_count = len(self.persistent())
        if total == 0:
            return "No drift traces recorded."
        return (
            f"{total} field trace(s) recorded; "
            f"{persistent_count} persistent (>=2 occurrences)."
        )


def build_trace(results_over_time: List[List[DriftResult]]) -> TraceReport:
    """Build a TraceReport from a list of result snapshots.

    Each inner list represents the drift results from one run.
    """
    if results_over_time is None:
        raise TracerError("results_over_time must not be None")

    counts: Dict[tuple, int] = {}

    for run in results_over_time:
        if run is None:
            raise TracerError("Individual run list must not be None")
        for result in run:
            for f in result.drifted_fields:
                key = (result.service, f)
                counts[key] = counts.get(key, 0) + 1

    traces = [
        FieldTrace(service=svc, field_name=fn, occurrences=cnt)
        for (svc, fn), cnt in sorted(counts.items())
    ]
    return TraceReport(traces=traces)
