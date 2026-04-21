"""scorer_ranker.py — ranks services by their drift score descending."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.scorer import ScoredResult, ScoredReport


class ScorerRankerError(Exception):
    """Raised when ranking fails."""


@dataclass
class RankedScoredResult:
    service: str
    score: float
    rank: int
    has_drift: bool

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "score": self.score,
            "rank": self.rank,
            "has_drift": self.has_drift,
        }


@dataclass
class ScorerRankedReport:
    results: List[RankedScoredResult] = field(default_factory=list)

    def top(self, n: int) -> List[RankedScoredResult]:
        if n < 0:
            raise ScorerRankerError("n must be non-negative")
        return self.results[:n]

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]


def rank_scored_report(report: Optional[ScoredReport]) -> ScorerRankedReport:
    """Rank a ScoredReport by score descending, assigning 1-based rank."""
    if report is None:
        raise ScorerRankerError("report must not be None")

    sorted_results = sorted(report.results, key=lambda r: r.score, reverse=True)

    ranked = [
        RankedScoredResult(
            service=r.service,
            score=r.score,
            rank=idx + 1,
            has_drift=r.has_drift,
        )
        for idx, r in enumerate(sorted_results)
    ]

    return ScorerRankedReport(results=ranked)
