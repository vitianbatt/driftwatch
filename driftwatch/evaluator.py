"""evaluator.py — evaluates drift results against threshold rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class EvaluatorError(Exception):
    """Raised when evaluation configuration is invalid."""


@dataclass
class ThresholdRule:
    """A rule that triggers when drift field count meets or exceeds a threshold."""

    name: str
    min_drift_fields: int
    tag: str = "threshold-breach"

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise EvaluatorError("ThresholdRule name must not be empty")
        if self.min_drift_fields < 1:
            raise EvaluatorError("min_drift_fields must be at least 1")
        if not self.tag or not self.tag.strip():
            raise EvaluatorError("ThresholdRule tag must not be empty")


@dataclass
class EvaluatedResult:
    """A drift result decorated with triggered threshold rules."""

    result: DriftResult
    triggered: List[ThresholdRule] = field(default_factory=list)

    def has_breach(self) -> bool:
        return len(self.triggered) > 0

    def breach_names(self) -> List[str]:
        return [r.name for r in self.triggered]

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "has_drift": self.result.has_drift,
            "drift_field_count": len(self.result.diffs),
            "has_breach": self.has_breach(),
            "triggered_rules": self.breach_names(),
        }


def evaluate_results(
    results: List[DriftResult],
    rules: List[ThresholdRule],
) -> List[EvaluatedResult]:
    """Evaluate each result against the provided threshold rules."""
    if results is None:
        raise EvaluatorError("results must not be None")
    if rules is None:
        raise EvaluatorError("rules must not be None")

    evaluated: List[EvaluatedResult] = []
    for result in results:
        drift_count = len(result.diffs)
        triggered = [
            rule for rule in rules if drift_count >= rule.min_drift_fields
        ]
        evaluated.append(EvaluatedResult(result=result, triggered=triggered))
    return evaluated
