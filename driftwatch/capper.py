"""capper.py – cap the number of drift results per service reported.

Useful when a single misconfigured service would otherwise flood reports.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class CapperError(Exception):
    """Raised when capper configuration or input is invalid."""


@dataclass
class CapConfig:
    max_diffs: int = 5

    def __post_init__(self) -> None:
        if self.max_diffs < 1:
            raise CapperError("max_diffs must be at least 1")


@dataclass
class CappedResult:
    service: str
    drifted_fields: List[str]
    was_capped: bool
    original_count: int

    def has_drift(self) -> bool:
        return len(self.drifted_fields) > 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "drifted_fields": self.drifted_fields,
            "was_capped": self.was_capped,
            "original_count": self.original_count,
        }


@dataclass
class CappedReport:
    results: List[CappedResult] = field(default_factory=list)

    def total_capped(self) -> int:
        return sum(1 for r in self.results if r.was_capped)

    def summary(self) -> str:
        total = len(self.results)
        capped = self.total_capped()
        if total == 0:
            return "No results to cap."
        return f"{total} service(s) processed; {capped} capped."


def cap_results(
    results: Optional[List[DriftResult]],
    config: Optional[CapConfig] = None,
) -> CappedReport:
    """Apply per-service diff cap to a list of DriftResult objects."""
    if results is None:
        raise CapperError("results must not be None")
    if config is None:
        config = CapConfig()

    capped: List[CappedResult] = []
    for r in results:
        original = list(r.drifted_fields)
        original_count = len(original)
        was_capped = original_count > config.max_diffs
        trimmed = original[: config.max_diffs]
        capped.append(
            CappedResult(
                service=r.service,
                drifted_fields=trimmed,
                was_capped=was_capped,
                original_count=original_count,
            )
        )
    return CappedReport(results=capped)
