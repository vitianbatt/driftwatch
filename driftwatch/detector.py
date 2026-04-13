"""detector.py — identifies newly appearing or disappearing drift fields across runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class DetectorError(Exception):
    """Raised when detection fails."""


@dataclass
class DetectedChange:
    service: str
    appeared: List[str] = field(default_factory=list)
    disappeared: List[str] = field(default_factory=list)

    def has_change(self) -> bool:
        return bool(self.appeared or self.disappeared)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "appeared": sorted(self.appeared),
            "disappeared": sorted(self.disappeared),
        }


@dataclass
class DetectionReport:
    changes: List[DetectedChange] = field(default_factory=list)

    def any_changes(self) -> bool:
        return any(c.has_change() for c in self.changes)

    def summary(self) -> str:
        changed = [c for c in self.changes if c.has_change()]
        if not changed:
            return "No drift changes detected."
        lines = [f"{len(changed)} service(s) with drift changes:"]
        for c in changed:
            if c.appeared:
                lines.append(f"  {c.service}: appeared {c.appeared}")
            if c.disappeared:
                lines.append(f"  {c.service}: disappeared {c.disappeared}")
        return "\n".join(lines)


def detect_changes(
    previous: List[DriftResult],
    current: List[DriftResult],
) -> DetectionReport:
    """Compare two snapshots of DriftResults and report field-level changes."""
    if previous is None or current is None:
        raise DetectorError("previous and current results must not be None")

    prev_map: Dict[str, set] = {
        r.service: set(r.drifted_fields) for r in previous
    }
    curr_map: Dict[str, set] = {
        r.service: set(r.drifted_fields) for r in current
    }

    all_services = set(prev_map) | set(curr_map)
    changes: List[DetectedChange] = []

    for svc in sorted(all_services):
        prev_fields = prev_map.get(svc, set())
        curr_fields = curr_map.get(svc, set())
        appeared = sorted(curr_fields - prev_fields)
        disappeared = sorted(prev_fields - curr_fields)
        changes.append(DetectedChange(service=svc, appeared=appeared, disappeared=disappeared))

    return DetectionReport(changes=changes)
