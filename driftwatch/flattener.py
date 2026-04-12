"""Flattener: converts nested drift results into flat key-value records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from driftwatch.comparator import DriftResult


class FlattenerError(Exception):
    """Raised when flattening fails."""


@dataclass
class FlatRecord:
    service: str
    key: str
    expected: Any
    actual: Any
    drift_type: str  # "missing" | "extra" | "changed"

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "key": self.key,
            "expected": self.expected,
            "actual": self.actual,
            "drift_type": self.drift_type,
        }


@dataclass
class FlatReport:
    records: list[FlatRecord] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.records)

    def services(self) -> list[str]:
        seen: list[str] = []
        for r in self.records:
            if r.service not in seen:
                seen.append(r.service)
        return seen

    def for_service(self, service: str) -> list[FlatRecord]:
        return [r for r in self.records if r.service == service]

    def summary(self) -> str:
        if not self.records:
            return "no drift records"
        return f"{len(self.records)} flat record(s) across {len(self.services())} service(s)"


def flatten_results(results: list[DriftResult]) -> FlatReport:
    """Expand each DriftResult's diffs into individual FlatRecord entries."""
    if results is None:
        raise FlattenerError("results must not be None")

    records: list[FlatRecord] = []
    for result in results:
        for diff in result.diffs:
            records.append(
                FlatRecord(
                    service=result.service,
                    key=diff.key,
                    expected=diff.expected,
                    actual=diff.actual,
                    drift_type=diff.diff_type,
                )
            )
    return FlatReport(records=records)
