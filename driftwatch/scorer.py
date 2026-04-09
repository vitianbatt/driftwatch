"""scorer.py — assigns a numeric drift score to a collection of DriftResults."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from driftwatch.comparator import DriftResult


class ScorerError(Exception):
    """Raised when scoring cannot be completed."""


# Weights used when computing the aggregate score.
_WEIGHT_MISSING = 3
_WEIGHT_EXTRA = 1
_WEIGHT_CHANGED = 2


@dataclass
class ScoredReport:
    """Holds per-service scores and an overall aggregate."""

    scores: dict[str, int] = field(default_factory=dict)
    total: int = 0
    service_count: int = 0

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def average(self) -> float:
        """Return mean score across all services (0.0 when no services)."""
        if self.service_count == 0:
            return 0.0
        return self.total / self.service_count

    def highest(self) -> tuple[str, int] | None:
        """Return (service, score) for the worst offender, or None."""
        if not self.scores:
            return None
        svc = max(self.scores, key=lambda k: self.scores[k])
        return svc, self.scores[svc]

    def summary(self) -> str:
        if self.service_count == 0:
            return "No services scored."
        worst = self.highest()
        assert worst is not None
        return (
            f"{self.service_count} service(s) scored | "
            f"total={self.total} avg={self.average():.1f} "
            f"worst='{worst[0]}'({worst[1]})"
        )


def _score_result(result: DriftResult) -> int:
    """Compute a single DriftResult's drift score."""
    diffs = result.diffs or []
    score = 0
    for diff in diffs:
        kind = getattr(diff, "kind", "").lower()
        if kind == "missing":
            score += _WEIGHT_MISSING
        elif kind == "extra":
            score += _WEIGHT_EXTRA
        elif kind == "changed":
            score += _WEIGHT_CHANGED
        else:
            score += 1  # unknown kind — minimal penalty
    return score


def score_results(results: List[DriftResult]) -> ScoredReport:
    """Score every result and return a ScoredReport.

    Args:
        results: List of DriftResult objects to evaluate.

    Returns:
        ScoredReport populated with per-service scores.

    Raises:
        ScorerError: If *results* is None.
    """
    if results is None:
        raise ScorerError("results must not be None")

    scores: dict[str, int] = {}
    for r in results:
        svc = r.service
        scores[svc] = _score_result(r)

    total = sum(scores.values())
    return ScoredReport(scores=scores, total=total, service_count=len(scores))
