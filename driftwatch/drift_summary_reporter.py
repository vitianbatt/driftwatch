"""Human-readable and JSON summary reporter for drift runs."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Sequence

from driftwatch.comparator import DriftResult


class DriftSummaryReporterError(Exception):
    """Raised for invalid inputs to the summary reporter."""


class SummaryFormat(str, Enum):
    TEXT = "text"
    JSON = "json"


@dataclass(frozen=True)
class DriftSummaryReport:
    total: int
    drifted: int
    clean: int
    drifted_services: list[str] = field(default_factory=list)

    @property
    def drift_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return round(self.drifted / self.total, 4)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "drifted": self.drifted,
            "clean": self.clean,
            "drift_rate": self.drift_rate,
            "drifted_services": self.drifted_services,
        }


def build_summary(results: Sequence[DriftResult]) -> DriftSummaryReport:
    """Build a DriftSummaryReport from a sequence of DriftResult objects."""
    if results is None:
        raise DriftSummaryReporterError("results must not be None")
    drifted = [r for r in results if r.diffs]
    clean = [r for r in results if not r.diffs]
    return DriftSummaryReport(
        total=len(results),
        drifted=len(drifted),
        clean=len(clean),
        drifted_services=sorted(r.service for r in drifted),
    )


def generate_summary_report(results: Sequence[DriftResult], fmt: SummaryFormat = SummaryFormat.TEXT) -> str:
    """Return a formatted summary report string."""
    report = build_summary(results)
    if fmt == SummaryFormat.JSON:
        return json.dumps(report.to_dict(), indent=2)
    lines = [
        f"Total services checked : {report.total}",
        f"Drifted                : {report.drifted}",
        f"Clean                  : {report.clean}",
        f"Drift rate             : {report.drift_rate:.1%}",
    ]
    if report.drifted_services:
        lines.append("Drifted services       : " + ", ".join(report.drifted_services))
    return "\n".join(lines)
