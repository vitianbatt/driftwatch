"""weigher.py – assigns numeric weights to drift results based on field importance."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class WeigherError(Exception):
    """Raised when weight configuration is invalid."""


@dataclass
class WeightMap:
    """Maps field names to importance weights (positive floats)."""

    weights: Dict[str, float]
    default_weight: float = 1.0

    def __post_init__(self) -> None:
        if self.default_weight <= 0:
            raise WeigherError("default_weight must be positive")
        for fname, w in self.weights.items():
            if not fname or not fname.strip():
                raise WeigherError("Field name in weight map must not be empty")
            if w <= 0:
                raise WeigherError(
                    f"Weight for field '{fname}' must be positive, got {w}"
                )

    def get(self, field_name: str) -> float:
        """Return weight for *field_name*, falling back to default."""
        return self.weights.get(field_name, self.default_weight)


@dataclass
class WeighedResult:
    """A drift result decorated with a total importance score."""

    service: str
    drifted_fields: List[str]
    score: float
    raw: DriftResult

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "drifted_fields": list(self.drifted_fields),
            "score": self.score,
        }


@dataclass
class WeighedReport:
    """Collection of weighed results."""

    results: List[WeighedResult] = field(default_factory=list)

    def total_score(self) -> float:
        return sum(r.score for r in self.results)

    def top(self, n: int = 5) -> List[WeighedResult]:
        return sorted(self.results, key=lambda r: r.score, reverse=True)[:n]


def weigh_results(
    results: Optional[List[DriftResult]],
    weight_map: WeightMap,
) -> WeighedReport:
    """Compute an importance score for every drift result.

    Score = sum of weights for each drifted field.
    Clean results receive a score of 0.0.
    """
    if results is None:
        raise WeigherError("results must not be None")

    weighed: List[WeighedResult] = []
    for r in results:
        drifted = list(r.diffs.keys()) if r.diffs else []
        score = sum(weight_map.get(f) for f in drifted)
        weighed.append(
            WeighedResult(
                service=r.service,
                drifted_fields=drifted,
                score=score,
                raw=r,
            )
        )
    return WeighedReport(results=weighed)
