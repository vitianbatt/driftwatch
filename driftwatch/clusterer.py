"""clusterer.py – groups DriftResults into clusters based on shared drifted fields."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class ClustererError(Exception):
    """Raised when clustering fails."""


@dataclass
class Cluster:
    label: str
    common_fields: List[str]
    results: List[DriftResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "common_fields": sorted(self.common_fields),
            "services": sorted(self.service_names()),
            "size": len(self),
        }


@dataclass
class ClusteredReport:
    clusters: Dict[str, Cluster] = field(default_factory=dict)
    unclustered: List[DriftResult] = field(default_factory=list)

    def cluster_names(self) -> List[str]:
        return sorted(self.clusters.keys())

    def total(self) -> int:
        return sum(len(c) for c in self.clusters.values()) + len(self.unclustered)

    def summary(self) -> str:
        parts = [f"{len(self.clusters)} cluster(s), {self.total()} result(s) total"]
        for name in self.cluster_names():
            c = self.clusters[name]
            parts.append(f"  [{name}] fields={c.common_fields} services={c.service_names()}")
        if self.unclustered:
            parts.append(f"  unclustered: {[r.service for r in self.unclustered]}")
        return "\n".join(parts)


def build_clusters(
    results: Optional[List[DriftResult]],
    min_shared_fields: int = 1,
) -> ClusteredReport:
    """Cluster results that share at least *min_shared_fields* drifted field names."""
    if results is None:
        raise ClustererError("results must not be None")
    if min_shared_fields < 1:
        raise ClustererError("min_shared_fields must be >= 1")

    drift_results = [r for r in results if r.diffs]
    clean_results = [r for r in results if not r.diffs]

    # Build field -> [result] mapping
    field_map: Dict[str, List[DriftResult]] = {}
    for r in drift_results:
        for d in r.diffs:
            field_map.setdefault(d.field, []).append(r)

    # Identify clusters: group results sharing >= min_shared_fields fields
    assigned: set = set()
    clusters: Dict[str, Cluster] = {}

    for f_name, members in sorted(field_map.items()):
        if len(members) < 2:
            continue
        shared = frozenset(r.service for r in members)
        label = f"cluster:{f_name}"
        if label not in clusters:
            clusters[label] = Cluster(label=label, common_fields=[f_name], results=list(members))
        assigned.update(shared)

    unclustered = [r for r in drift_results if r.service not in assigned] + clean_results
    return ClusteredReport(clusters=clusters, unclustered=unclustered)
