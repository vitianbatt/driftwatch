"""Summarizer: produce a concise human-readable summary of drift results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from driftwatch.comparator import DriftResult


class SummarizerError(Exception):
    """Raised when summarization fails."""


@dataclass
class ServiceSummary:
    service: str
    total_fields: int
    drifted_fields: List[str]

    @property
    def has_drift(self) -> bool:
        return len(self.drifted_fields) > 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "total_fields": self.total_fields,
            "drifted_fields": list(self.drifted_fields),
            "has_drift": self.has_drift,
        }


@dataclass
class SummaryReport:
    services: List[ServiceSummary] = field(default_factory=list)

    @property
    def total_services(self) -> int:
        return len(self.services)

    @property
    def drifted_services(self) -> List[ServiceSummary]:
        return [s for s in self.services if s.has_drift]

    @property
    def clean_services(self) -> List[ServiceSummary]:
        return [s for s in self.services if not s.has_drift]

    def text(self) -> str:
        lines = [f"Drift Summary: {len(self.drifted_services)}/{self.total_services} services drifted"]
        for svc in self.services:
            status = "DRIFT" if svc.has_drift else "OK"
            lines.append(f"  [{status}] {svc.service} ({svc.total_fields} fields checked)")
            for f_name in svc.drifted_fields:
                lines.append(f"         - {f_name}")
        return "\n".join(lines)


def summarize(results: List[DriftResult]) -> SummaryReport:
    """Build a SummaryReport from a list of DriftResult objects."""
    if results is None:
        raise SummarizerError("results must not be None")

    summaries: List[ServiceSummary] = []
    for result in results:
        drifted = list(result.diffs.keys()) if result.diffs else []
        total = len(result.spec) if result.spec else 0
        summaries.append(
            ServiceSummary(
                service=result.service,
                total_fields=total,
                drifted_fields=drifted,
            )
        )
    return SummaryReport(services=summaries)
