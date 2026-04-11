"""Profiler module: tracks how often each field drifts across runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from driftwatch.comparator import DriftResult


class ProfilerError(Exception):
    """Raised when profiling fails."""


@dataclass
class FieldProfile:
    field_name: str
    drift_count: int = 0
    seen_in_services: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "drift_count": self.drift_count,
            "seen_in_services": sorted(self.seen_in_services),
        }


@dataclass
class ProfileReport:
    profiles: Dict[str, FieldProfile] = field(default_factory=dict)

    @property
    def total_fields_tracked(self) -> int:
        return len(self.profiles)

    def top(self, n: int = 5) -> List[FieldProfile]:
        """Return up to *n* fields sorted by drift_count descending."""
        if n < 0:
            raise ProfilerError("n must be non-negative")
        sorted_profiles = sorted(
            self.profiles.values(), key=lambda p: p.drift_count, reverse=True
        )
        return sorted_profiles[:n]

    def summary(self) -> str:
        if not self.profiles:
            return "No field drift recorded."
        lines = [f"Field drift profile ({self.total_fields_tracked} fields):"]
        for p in self.top(10):
            services = ", ".join(p.seen_in_services)
            lines.append(f"  {p.field_name}: {p.drift_count} drift(s) [{services}]")
        return "\n".join(lines)


def build_profile(results: List[DriftResult]) -> ProfileReport:
    """Build a :class:`ProfileReport` from a list of drift results."""
    if results is None:
        raise ProfilerError("results must not be None")

    report = ProfileReport()
    for result in results:
        if not isinstance(result, DriftResult):
            raise ProfilerError(f"Expected DriftResult, got {type(result).__name__}")
        for diff in result.diffs:
            fname = diff.field
            if fname not in report.profiles:
                report.profiles[fname] = FieldProfile(field_name=fname)
            report.profiles[fname].drift_count += 1
            if result.service not in report.profiles[fname].seen_in_services:
                report.profiles[fname].seen_in_services.append(result.service)
    return report
