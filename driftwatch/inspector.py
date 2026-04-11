"""inspector.py — field-level inspection of drift results, producing per-field occurrence counts and service lists."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class InspectorError(Exception):
    """Raised when inspection fails."""


@dataclass
class FieldOccurrence:
    field_name: str
    count: int
    services: List[str]

    def to_dict(self) -> dict:
        return {
            "field": self.field_name,
            "count": self.count,
            "services": sorted(self.services),
        }


@dataclass
class InspectionReport:
    occurrences: List[FieldOccurrence] = field(default_factory=list)

    def total_fields_tracked(self) -> int:
        return len(self.occurrences)

    def most_common(self, n: int = 5) -> List[FieldOccurrence]:
        return sorted(self.occurrences, key=lambda o: o.count, reverse=True)[:n]

    def lookup(self, field_name: str) -> Optional[FieldOccurrence]:
        for occ in self.occurrences:
            if occ.field_name == field_name:
                return occ
        return None

    def summary(self) -> str:
        if not self.occurrences:
            return "No drift fields detected across any service."
        lines = [f"Inspected {self.total_fields_tracked()} unique drifted field(s):"]
        for occ in self.most_common():
            lines.append(f"  {occ.field_name}: {occ.count} service(s) -> {', '.join(sorted(occ.services))}")
        return "\n".join(lines)


def build_inspection(results: List[DriftResult]) -> InspectionReport:
    """Aggregate drift fields across all results into an InspectionReport."""
    if results is None:
        raise InspectorError("results must not be None")

    counts: Dict[str, List[str]] = {}
    for result in results:
        for diff in result.diffs:
            fname = diff.field
            counts.setdefault(fname, [])
            if result.service not in counts[fname]:
                counts[fname].append(result.service)

    occurrences = [
        FieldOccurrence(field_name=fname, count=len(svcs), services=svcs)
        for fname, svcs in counts.items()
    ]
    return InspectionReport(occurrences=occurrences)
