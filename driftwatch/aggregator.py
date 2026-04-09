"""Aggregates drift results across multiple services into a structured summary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class AggregatorError(Exception):
    """Raised when aggregation fails."""


@dataclass
class ServiceSummary:
    service: str
    has_drift: bool
    drift_field_count: int
    severity: Severity

    def to_dict(self) -> Dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift,
            "drift_field_count": self.drift_field_count,
            "severity": self.severity.value,
        }


@dataclass
class AggregateReport:
    total_services: int
    drifted_services: int
    clean_services: int
    severity_counts: Dict[str, int]
    summaries: List[ServiceSummary] = field(default_factory=list)

    @property
    def drift_rate(self) -> float:
        if self.total_services == 0:
            return 0.0
        return round(self.drifted_services / self.total_services, 4)

    def to_dict(self) -> Dict:
        return {
            "total_services": self.total_services,
            "drifted_services": self.drifted_services,
            "clean_services": self.clean_services,
            "drift_rate": self.drift_rate,
            "severity_counts": self.severity_counts,
            "summaries": [s.to_dict() for s in self.summaries],
        }


def aggregate(results: List[DriftResult]) -> AggregateReport:
    """Build an AggregateReport from a list of DriftResults."""
    if results is None:
        raise AggregatorError("results must not be None")

    severity_counts: Dict[str, int] = {s.value: 0 for s in Severity}
    summaries: List[ServiceSummary] = []

    for result in results:
        sev = _result_severity(result)
        severity_counts[sev.value] += 1
        summaries.append(
            ServiceSummary(
                service=result.service,
                has_drift=result.has_drift,
                drift_field_count=len(result.missing_keys) + len(result.extra_keys) + len(result.changed_keys),
                severity=sev,
            )
        )

    drifted = sum(1 for s in summaries if s.has_drift)
    return AggregateReport(
        total_services=len(summaries),
        drifted_services=drifted,
        clean_services=len(summaries) - drifted,
        severity_counts=severity_counts,
        summaries=summaries,
    )
