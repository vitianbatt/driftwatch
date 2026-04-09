"""sorter.py — sort drift results by various criteria."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List

from driftwatch.comparator import DriftResult


class SorterError(Exception):
    """Raised when sorting fails."""


class SortBy(str, Enum):
    SERVICE = "service"
    DRIFT_COUNT = "drift_count"
    SEVERITY = "severity"  # number of drifted fields, descending


@dataclass
class SortedReport:
    results: List[DriftResult]
    sort_by: SortBy
    ascending: bool

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]


def _drift_count(result: DriftResult) -> int:
    return len(result.drifted_fields)


def sort_results(
    results: List[DriftResult],
    sort_by: SortBy = SortBy.SERVICE,
    ascending: bool = True,
) -> SortedReport:
    """Return a SortedReport with results ordered by *sort_by*.

    Args:
        results:   List of DriftResult objects to sort.
        sort_by:   The field to sort on.
        ascending: When True, sort in ascending order.

    Raises:
        SorterError: If *results* is None or an unsupported sort_by is given.
    """
    if results is None:
        raise SorterError("results must not be None")

    if sort_by == SortBy.SERVICE:
        key = lambda r: r.service.lower()
    elif sort_by in (SortBy.DRIFT_COUNT, SortBy.SEVERITY):
        key = _drift_count
    else:
        raise SorterError(f"Unsupported sort_by value: {sort_by!r}")

    sorted_list = sorted(results, key=key, reverse=not ascending)
    return SortedReport(results=sorted_list, sort_by=sort_by, ascending=ascending)
