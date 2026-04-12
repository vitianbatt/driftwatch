"""Score DriftResult objects by severity of detected drift."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff

# Weight constants
_MISSING_WEIGHT = 3
_EXTRA_WEIGHT = 1
_CHANGED_WEIGHT = 2
_DEFAULT_WEIGHT = 2


class ScorerError(Exception):
    """Raised when scoring fails."""


@dataclass
class ScoredResult:
    service: str
    score: int
    priority: str
    drift_fields: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "score": self.score,
            "priority": self.priority,
            "drift_fields": self.drift_fields,
        }


@dataclass
class ScoredReport:
    results: list[ScoredResult] = field(default_factory=list)

    @property
    def average(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.score for r in self.results) / len(self.results)

    @property
    def highest(self) -> Optional[ScoredResult]:
        if not self.results:
            return None
        return max(self.results, key=lambda r: r.score)

    def summary(self) -> str:
        if not self.results:
            return "No results to score."
        return (
            f"{len(self.results)} service(s) scored — "
            f"avg {self.average:.1f}, "
            f"highest: {self.highest.service} ({self.highest.score})"
        )


def _compute_score(diffs: list[FieldDiff]) -> int:
    score = 0
    for d in diffs:
        if d.expected is None:
            score += _EXTRA_WEIGHT
        elif d.actual is None:
            score += _MISSING_WEIGHT
        else:
            score += _CHANGED_WEIGHT
    return score


def _score_to_priority(score: int) -> str:
    if score == 0:
        return "low"
    if score <= 2:
        return "low"
    if score <= 5:
        return "normal"
    return "high"


def score_results(results: list[DriftResult]) -> ScoredReport:
    """Score each DriftResult and return a ScoredReport."""
    if results is None:
        raise ScorerError("results must not be None")
    scored = []
    for r in results:
        s = _compute_score(r.diffs)
        scored.append(ScoredResult(
            service=r.service,
            score=s,
            priority=_score_to_priority(s),
            drift_fields=[d.field for d in r.diffs],
        ))
    return ScoredReport(results=scored)
