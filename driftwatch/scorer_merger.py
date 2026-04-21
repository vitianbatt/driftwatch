"""Merge two ScoredReports, resolving conflicts by keeping the higher score."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.scorer import ScoredResult, ScoredReport


class ScorerMergerError(Exception):
    """Raised when merging fails."""


@dataclass
class MergedScoredResult:
    service: str
    score: float
    drifted_fields: List[str] = field(default_factory=list)
    source: str = "merged"
    conflict: bool = False

    def has_drift(self) -> bool:
        return len(self.drifted_fields) > 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "score": self.score,
            "drifted_fields": self.drifted_fields,
            "source": self.source,
            "conflict": self.conflict,
        }


@dataclass
class MergedScoredReport:
    results: List[MergedScoredResult] = field(default_factory=list)
    conflict_count: int = 0

    def total(self) -> int:
        return len(self.results)

    def average_score(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.score for r in self.results) / len(self.results)

    def summary(self) -> str:
        return (
            f"{self.total()} service(s) merged; "
            f"{self.conflict_count} conflict(s); "
            f"avg score {self.average_score():.2f}"
        )


def merge_scored_reports(
    primary: ScoredReport,
    secondary: ScoredReport,
) -> MergedScoredReport:
    """Merge two ScoredReports. Conflicts resolved by keeping higher score."""
    if primary is None or secondary is None:
        raise ScorerMergerError("Both primary and secondary reports are required.")

    primary_map: dict = {r.service: r for r in primary.results}
    secondary_map: dict = {r.service: r for r in secondary.results}

    merged: List[MergedScoredResult] = []
    conflict_count = 0

    all_services = sorted(set(primary_map) | set(secondary_map))

    for service in all_services:
        p: Optional[ScoredResult] = primary_map.get(service)
        s: Optional[ScoredResult] = secondary_map.get(service)

        if p is not None and s is None:
            merged.append(
                MergedScoredResult(
                    service=service,
                    score=p.score,
                    drifted_fields=list(p.drifted_fields),
                    source="primary",
                    conflict=False,
                )
            )
        elif s is not None and p is None:
            merged.append(
                MergedScoredResult(
                    service=service,
                    score=s.score,
                    drifted_fields=list(s.drifted_fields),
                    source="secondary",
                    conflict=False,
                )
            )
        else:
            conflict = p.score != s.score or set(p.drifted_fields) != set(s.drifted_fields)
            if conflict:
                conflict_count += 1
            winner = p if p.score >= s.score else s
            merged.append(
                MergedScoredResult(
                    service=service,
                    score=winner.score,
                    drifted_fields=list(winner.drifted_fields),
                    source="primary" if winner is p else "secondary",
                    conflict=conflict,
                )
            )

    return MergedScoredReport(results=merged, conflict_count=conflict_count)
