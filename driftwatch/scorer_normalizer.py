"""Normalizes scored results so scores fall within a configurable [0.0, 1.0] range."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.scorer import ScoredResult


class ScorerNormalizerError(Exception):
    """Raised when normalization cannot be performed."""


@dataclass
class NormalizedScoredResult:
    service: str
    raw_score: float
    normalized_score: float
    drifted_fields: List[str] = field(default_factory=list)

    def has_drift(self) -> bool:
        return bool(self.drifted_fields)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "raw_score": self.raw_score,
            "normalized_score": round(self.normalized_score, 4),
            "drifted_fields": list(self.drifted_fields),
            "has_drift": self.has_drift(),
        }


@dataclass
class NormalizedScoredReport:
    results: List[NormalizedScoredResult]
    min_raw: float
    max_raw: float

    def top(self, n: int = 5) -> List[NormalizedScoredResult]:
        return sorted(self.results, key=lambda r: r.normalized_score, reverse=True)[:n]


def normalize_scores(
    results: Optional[List[ScoredResult]],
    floor: float = 0.0,
    ceiling: float = 1.0,
) -> NormalizedScoredReport:
    """Normalize raw scores into [floor, ceiling].  If all scores are equal
    every result receives the floor value."""
    if results is None:
        raise ScorerNormalizerError("results must not be None")
    if floor >= ceiling:
        raise ScorerNormalizerError(
            f"floor ({floor}) must be less than ceiling ({ceiling})"
        )

    if not results:
        return NormalizedScoredReport(results=[], min_raw=0.0, max_raw=0.0)

    raw_scores = [r.score for r in results]
    min_raw = min(raw_scores)
    max_raw = max(raw_scores)
    span = max_raw - min_raw

    normalized: List[NormalizedScoredResult] = []
    for r in results:
        if span == 0.0:
            norm = floor
        else:
            norm = floor + (r.score - min_raw) / span * (ceiling - floor)
        normalized.append(
            NormalizedScoredResult(
                service=r.service,
                raw_score=r.score,
                normalized_score=norm,
                drifted_fields=list(r.drifted_fields),
            )
        )

    return NormalizedScoredReport(results=normalized, min_raw=min_raw, max_raw=max_raw)
