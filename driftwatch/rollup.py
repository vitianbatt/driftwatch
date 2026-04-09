"""Rollup: aggregate multiple DriftResults into a summary report."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class RollupError(Exception):
    """Raised when rollup aggregation fails."""


@dataclass
class RollupReport:
    total: int
    clean: int
    drifted: int
    by_severity: Dict[str, int]
    services: List[str]
    drifted_services: List[str]

    def has_any_drift(self) -> bool:
        return self.drifted > 0

    def summary(self) -> str:
        lines = [
            f"Total services checked : {self.total}",
            f"Clean                  : {self.clean}",
            f"Drifted                : {self.drifted}",
        ]
        for sev in (Severity.HIGH, Severity.MEDIUM, Severity.LOW):
            count = self.by_severity.get(sev.value, 0)
            lines.append(f"  {sev.value:<8}: {count}")
        if self.drifted_services:
            lines.append("Drifted services       : " + ", ".join(self.drifted_services))
        return "\n".join(lines)


def build_rollup(results: List[DriftResult]) -> RollupReport:
    """Aggregate a list of DriftResults into a RollupReport."""
    if results is None:
        raise RollupError("results must not be None")

    total = len(results)
    clean = 0
    drifted = 0
    by_severity: Dict[str, int] = {s.value: 0 for s in Severity}
    services: List[str] = []
    drifted_services: List[str] = []

    for result in results:
        services.append(result.service)
        sev = _result_severity(result)
        by_severity[sev.value] += 1
        if result.diffs:
            drifted += 1
            drifted_services.append(result.service)
        else:
            clean += 1

    return RollupReport(
        total=total,
        clean=clean,
        drifted=drifted,
        by_severity=by_severity,
        services=services,
        drifted_services=drifted_services,
    )
