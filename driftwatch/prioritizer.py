"""Prioritizer: rank drift results by urgency based on severity and field count."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class PrioritizerError(Exception):
    """Raised when prioritization input is invalid."""


class Priority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass
class PrioritizedResult:
    result: DriftResult
    severity: Severity
    priority: Priority
    score: int

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "priority": self.priority.value,
            "severity": self.severity.value,
            "score": self.score,
            "drift_fields": self.result.missing_keys + self.result.extra_keys + self.result.changed_keys,
        }


def _compute_score(result: DriftResult, severity: Severity) -> int:
    """Compute a numeric urgency score for a drift result."""
    severity_weight = {Severity.LOW: 1, Severity.MEDIUM: 3, Severity.HIGH: 5}
    field_count = len(result.missing_keys) + len(result.extra_keys) + len(result.changed_keys)
    return severity_weight[severity] * max(field_count, 1)


def _score_to_priority(score: int) -> Priority:
    if score >= 15:
        return Priority.CRITICAL
    if score >= 6:
        return Priority.HIGH
    if score >= 3:
        return Priority.NORMAL
    return Priority.LOW


def prioritize(results: List[DriftResult]) -> List[PrioritizedResult]:
    """Return results sorted from highest to lowest urgency."""
    if results is None:
        raise PrioritizerError("results must not be None")

    prioritized: List[PrioritizedResult] = []
    for r in results:
        sev = _result_severity(r)
        score = _compute_score(r, sev)
        priority = _score_to_priority(score)
        prioritized.append(PrioritizedResult(result=r, severity=sev, priority=priority, score=score))

    prioritized.sort(key=lambda p: p.score, reverse=True)
    return prioritized
