"""Correlate drift results across multiple services to find shared root causes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class CorrelatorError(Exception):
    """Raised when correlation cannot be performed."""


@dataclass
class CorrelationGroup:
    """A group of services that share the same drifted field names."""

    fields: List[str]
    services: List[str]

    def size(self) -> int:
        return len(self.services)

    def to_dict(self) -> dict:
        return {"fields": sorted(self.fields), "services": sorted(self.services)}


@dataclass
class CorrelationReport:
    """Holds all correlation groups derived from a set of DriftResults."""

    groups: List[CorrelationGroup] = field(default_factory=list)

    def total_groups(self) -> int:
        return len(self.groups)

    def services_in_any_group(self) -> List[str]:
        seen: List[str] = []
        for g in self.groups:
            for s in g.services:
                if s not in seen:
                    seen.append(s)
        return sorted(seen)

    def summary(self) -> str:
        if not self.groups:
            return "No correlated drift groups found."
        lines = [f"{self.total_groups()} correlation group(s) detected:"]
        for i, g in enumerate(self.groups, 1):
            fields_str = ", ".join(sorted(g.fields))
            services_str = ", ".join(sorted(g.services))
            lines.append(f"  [{i}] fields=[{fields_str}] services=[{services_str}]")
        return "\n".join(lines)


def correlate(results: Optional[List[DriftResult]]) -> CorrelationReport:
    """Group services that share identical sets of drifted field names."""
    if results is None:
        raise CorrelatorError("results must not be None")

    # Map frozenset(field_names) -> list of service names
    bucket: Dict[frozenset, List[str]] = {}
    for r in results:
        if not r.drifted_fields:
            continue
        key = frozenset(r.drifted_fields)
        bucket.setdefault(key, []).append(r.service)

    groups = [
        CorrelationGroup(fields=list(k), services=v)
        for k, v in bucket.items()
        if len(v) > 1
    ]
    # Deterministic order: sort by field set then services
    groups.sort(key=lambda g: (sorted(g.fields), sorted(g.services)))
    return CorrelationReport(groups=groups)
