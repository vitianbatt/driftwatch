"""Aggregate scored results into a summary report with statistics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.scorer import ScoredResult


class ScorerAggregatorError(Exception):
    """Raised when aggregation fails."""


@dataclass
class AggregatedScoredReport:
    results: List[ScoredResult]
    total: int = field(init=False)
    drifted: int = field(init=False)
    clean: int = field(init=False)
    min_score: float = field(init=False)
    max_score: float = field(init=False)
    mean_score: float = field(init=False)

    def __post_init__(self) -> None:
        if self.results is None:
            raise ScorerAggregatorError("results must not be None")
        self.total = len(self.results)
        self.drifted = sum(1 for r in self.results if r.score > 0)
        self.clean = self.total - self.drifted
        scores = [r.score for r in self.results]
        self.min_score = min(scores) if scores else 0.0
        self.max_score = max(scores) if scores else 0.0
        self.mean_score = sum(scores) / len(scores) if scores else 0.0

    def drift_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.drifted / self.total

    def top(self, n: int = 5) -> List[ScoredResult]:
        """Return the n highest-scoring (most drifted) results."""
        return sorted(self.results, key=lambda r: r.score, reverse=True)[:n]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "drifted": self.drifted,
            "clean": self.clean,
            "min_score": self.min_score,
            "max_score": self.max_score,
            "mean_score": round(self.mean_score, 4),
            "drift_rate": round(self.drift_rate(), 4),
        }


def aggregate_scored(results: Optional[List[ScoredResult]]) -> AggregatedScoredReport:
    """Build an aggregated report from a list of ScoredResults."""
    if results is None:
        raise ScorerAggregatorError("results must not be None")
    return AggregatedScoredReport(results=results)
