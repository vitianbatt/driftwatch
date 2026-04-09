"""Group drift results by a chosen dimension (service, severity, tag)."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List

from driftwatch.comparator import DriftResult


class GrouperError(Exception):
    """Raised when grouping fails."""


class GroupBy(str, Enum):
    SERVICE = "service"
    SEVERITY = "severity"
    TAG = "tag"


@dataclass
class GroupedReport:
    dimension: str
    groups: Dict[str, List[DriftResult]] = field(default_factory=dict)

    def group_names(self) -> List[str]:
        return sorted(self.groups.keys())

    def size(self, group_name: str) -> int:
        return len(self.groups.get(group_name, []))

    def total(self) -> int:
        return sum(len(v) for v in self.groups.values())

    def summary(self) -> str:
        lines = [f"Grouped by '{self.dimension}' — {self.total()} result(s)"]
        for name in self.group_names():
            lines.append(f"  {name}: {self.size(name)}")
        return "\n".join(lines)


def _severity_label(result: DriftResult) -> str:
    """Return a simple severity bucket based on number of drifted fields."""
    count = len(result.diffs)
    if count == 0:
        return "low"
    if count <= 2:
        return "medium"
    return "high"


def group_results(
    results: List[DriftResult],
    by: GroupBy,
    tag_map: Dict[str, str] | None = None,
) -> GroupedReport:
    """Group *results* by the chosen *by* dimension.

    When *by* is ``GroupBy.TAG`` a *tag_map* of ``{service: tag}`` must be
    supplied; services absent from the map land in the ``"untagged"`` bucket.
    """
    if results is None:
        raise GrouperError("results must not be None")
    if by is GroupBy.TAG and tag_map is None:
        raise GrouperError("tag_map is required when grouping by tag")

    report = GroupedReport(dimension=by.value)

    for result in results:
        if by is GroupBy.SERVICE:
            key = result.service
        elif by is GroupBy.SEVERITY:
            key = _severity_label(result)
        else:  # TAG
            key = (tag_map or {}).get(result.service, "untagged")

        report.groups.setdefault(key, []).append(result)

    return report
