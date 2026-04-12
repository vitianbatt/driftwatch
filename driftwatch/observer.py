"""observer.py – tracks field-level change frequency across multiple drift results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class ObserverError(Exception):
    """Raised when observation inputs are invalid."""


@dataclass
class FieldObservation:
    field_name: str
    occurrences: int
    services: List[str]

    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "occurrences": self.occurrences,
            "services": sorted(self.services),
        }


@dataclass
class ObservationReport:
    observations: Dict[str, FieldObservation] = field(default_factory=dict)

    def field_names(self) -> List[str]:
        return sorted(self.observations.keys())

    def top(self, n: int = 5) -> List[FieldObservation]:
        sorted_obs = sorted(
            self.observations.values(),
            key=lambda o: o.occurrences,
            reverse=True,
        )
        return sorted_obs[:n]

    def total_tracked(self) -> int:
        return len(self.observations)

    def summary(self) -> str:
        if not self.observations:
            return "No field observations recorded."
        lines = [f"Observed {self.total_tracked()} distinct drifted field(s):"]
        for obs in self.top():
            lines.append(
                f"  {obs.field_name}: {obs.occurrences} occurrence(s) "
                f"across {len(obs.services)} service(s)"
            )
        return "\n".join(lines)


def observe(results: Optional[List[DriftResult]]) -> ObservationReport:
    """Build an ObservationReport from a list of DriftResult objects."""
    if results is None:
        raise ObserverError("results must not be None")

    report = ObservationReport()
    for result in results:
        if not result.diffs:
            continue
        for diff in result.diffs:
            fname = diff.field
            if fname not in report.observations:
                report.observations[fname] = FieldObservation(
                    field_name=fname, occurrences=0, services=[]
                )
            obs = report.observations[fname]
            obs.occurrences += 1
            if result.service not in obs.services:
                obs.services.append(result.service)
    return report
