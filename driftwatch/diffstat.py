"""diffstat.py — compute per-field drift statistics across a set of DriftResults."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class DiffStatError(Exception):
    """Raised when diffstat computation fails."""


@dataclass
class FieldStat:
    field_name: str
    occurrences: int = 0
    services: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "occurrences": self.occurrences,
            "services": sorted(self.services),
        }


@dataclass
class DiffStatReport:
    stats: Dict[str, FieldStat] = field(default_factory=dict)
    total_results: int = 0
    total_drifted: int = 0

    def most_common(self, n: int = 5) -> List[FieldStat]:
        sorted_stats = sorted(
            self.stats.values(), key=lambda s: s.occurrences, reverse=True
        )
        return sorted_stats[:n]

    def summary(self) -> str:
        if not self.stats:
            return "No drift fields recorded."
        top = self.most_common(3)
        lines = [f"Total results: {self.total_results}, drifted: {self.total_drifted}"]
        for s in top:
            lines.append(f"  {s.field_name}: {s.occurrences} occurrence(s)")
        return "\n".join(lines)


def build_diffstat(results: Optional[List[DriftResult]]) -> DiffStatReport:
    """Build a DiffStatReport from a list of DriftResults."""
    if results is None:
        raise DiffStatError("results must not be None")

    report = DiffStatReport(total_results=len(results))

    for result in results:
        if not isinstance(result, DriftResult):
            raise DiffStatError(f"Expected DriftResult, got {type(result).__name__}")
        drifted_fields = getattr(result, "drifted_fields", []) or []
        if drifted_fields:
            report.total_drifted += 1
        for fname in drifted_fields:
            if fname not in report.stats:
                report.stats[fname] = FieldStat(field_name=fname)
            report.stats[fname].occurrences += 1
            if result.service not in report.stats[fname].services:
                report.stats[fname].services.append(result.service)

    return report
