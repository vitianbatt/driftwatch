"""Projects DriftResult fields into a flat key-value view for downstream use."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class ProjectorError(Exception):
    """Raised when projection fails."""


@dataclass
class ProjectedField:
    name: str
    expected: Optional[str]
    actual: Optional[str]
    diff_type: str  # "missing", "extra", "changed"

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "expected": self.expected,
            "actual": self.actual,
            "diff_type": self.diff_type,
        }


@dataclass
class ProjectedResult:
    service: str
    fields: List[ProjectedField] = field(default_factory=list)

    def has_drift(self) -> bool:
        return len(self.fields) > 0

    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    def to_dict(self) -> Dict[str, object]:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "fields": [f.to_dict() for f in self.fields],
        }


def _diff_to_projected(d: FieldDiff) -> ProjectedField:
    diff_type = "changed"
    if d.expected is None:
        diff_type = "extra"
    elif d.actual is None:
        diff_type = "missing"
    return ProjectedField(
        name=d.field,
        expected=str(d.expected) if d.expected is not None else None,
        actual=str(d.actual) if d.actual is not None else None,
        diff_type=diff_type,
    )


def project_results(results: List[DriftResult]) -> List[ProjectedResult]:
    """Convert a list of DriftResults into ProjectedResults."""
    if results is None:
        raise ProjectorError("results must not be None")
    projected = []
    for r in results:
        fields = [_diff_to_projected(d) for d in (r.diffs or [])]
        projected.append(ProjectedResult(service=r.service, fields=fields))
    return projected
