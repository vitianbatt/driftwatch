"""scorer_comparator: compare two ScoredReports and surface score deltas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.scorer import ScoredReport, ScoredResult


class ScorerComparatorError(Exception):
    """Raised when comparison inputs are invalid."""


@dataclass
class ScoreDelta:
    service: str
    previous_score: float
    current_score: float

    @property
    def delta(self) -> float:
        return self.current_score - self.previous_score

    @property
    def improved(self) -> bool:
        return self.delta < 0

    @property
    def regressed(self) -> bool:
        return self.delta > 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "previous_score": self.previous_score,
            "current_score": self.current_score,
            "delta": round(self.delta, 4),
            "improved": self.improved,
            "regressed": self.regressed,
        }


@dataclass
class ScorerComparisonReport:
    deltas: List[ScoreDelta] = field(default_factory=list)
    new_services: List[str] = field(default_factory=list)
    dropped_services: List[str] = field(default_factory=list)

    def has_regressions(self) -> bool:
        return any(d.regressed for d in self.deltas)

    def summary(self) -> str:
        if not self.deltas and not self.new_services and not self.dropped_services:
            return "No changes between reports."
        parts = []
        regressions = [d for d in self.deltas if d.regressed]
        improvements = [d for d in self.deltas if d.improved]
        if regressions:
            parts.append(f"{len(regressions)} regression(s)")
        if improvements:
            parts.append(f"{len(improvements)} improvement(s)")
        if self.new_services:
            parts.append(f"{len(self.new_services)} new service(s)")
        if self.dropped_services:
            parts.append(f"{len(self.dropped_services)} dropped service(s)")
        return "; ".join(parts) + "."


def compare_scored_reports(
    previous: ScoredReport,
    current: ScoredReport,
) -> ScorerComparisonReport:
    if previous is None or current is None:
        raise ScorerComparatorError("Both previous and current reports must be provided.")

    prev_map: Dict[str, float] = {r.service: r.score for r in previous.results}
    curr_map: Dict[str, float] = {r.service: r.score for r in current.results}

    deltas: List[ScoreDelta] = []
    for service, curr_score in curr_map.items():
        if service in prev_map:
            if curr_score != prev_map[service]:
                deltas.append(ScoreDelta(
                    service=service,
                    previous_score=prev_map[service],
                    current_score=curr_score,
                ))

    new_services = [s for s in curr_map if s not in prev_map]
    dropped_services = [s for s in prev_map if s not in curr_map]

    return ScorerComparisonReport(
        deltas=deltas,
        new_services=new_services,
        dropped_services=dropped_services,
    )
