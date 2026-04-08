"""Filter and select drift results based on severity, service name, or drift status."""

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from driftwatch.comparator import DriftResult


class FilterError(Exception):
    """Raised when an invalid filter configuration is provided."""


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


_SEVERITY_THRESHOLDS = {
    Severity.LOW: 1,
    Severity.MEDIUM: 3,
    Severity.HIGH: 6,
}


def _result_severity(result: DriftResult) -> Severity:
    """Classify a DriftResult by the number of drifted fields."""
    count = len(result.missing_keys) + len(result.extra_keys) + len(result.changed_values)
    if count >= _SEVERITY_THRESHOLDS[Severity.HIGH]:
        return Severity.HIGH
    if count >= _SEVERITY_THRESHOLDS[Severity.MEDIUM]:
        return Severity.MEDIUM
    return Severity.LOW


def filter_results(
    results: List[DriftResult],
    *,
    only_drift: bool = False,
    service: Optional[str] = None,
    min_severity: Optional[Severity] = None,
) -> List[DriftResult]:
    """Return a filtered subset of *results*.

    Args:
        results: List of DriftResult objects to filter.
        only_drift: When True, exclude results with no drift.
        service: When provided, only include results whose service name
            contains this string (case-insensitive substring match).
        min_severity: When provided, only include results whose severity
            is at or above this level.

    Returns:
        Filtered list of DriftResult objects.

    Raises:
        FilterError: If *results* is not a list.
    """
    if not isinstance(results, list):
        raise FilterError(f"Expected a list of DriftResult, got {type(results).__name__}")

    filtered = results

    if only_drift:
        filtered = [r for r in filtered if r.has_drift()]

    if service is not None:
        needle = service.lower()
        filtered = [r for r in filtered if needle in r.service_name.lower()]

    if min_severity is not None:
        order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH]
        threshold_index = order.index(min_severity)
        filtered = [
            r for r in filtered
            if order.index(_result_severity(r)) >= threshold_index
        ]

    return filtered
