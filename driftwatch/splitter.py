"""splitter.py — splits a list of DriftResults into named partitions based on a routing map."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class SplitterError(Exception):
    """Raised when splitting configuration is invalid."""


@dataclass
class SplitReport:
    """Holds named partitions of DriftResults."""

    partitions: Dict[str, List[DriftResult]] = field(default_factory=dict)
    unmatched: List[DriftResult] = field(default_factory=list)

    def partition_names(self) -> List[str]:
        """Return sorted partition names."""
        return sorted(self.partitions.keys())

    def size(self, name: str) -> int:
        """Return number of results in a named partition (0 if missing)."""
        return len(self.partitions.get(name, []))

    def total(self) -> int:
        """Return total results across all partitions and unmatched."""
        return sum(len(v) for v in self.partitions.values()) + len(self.unmatched)

    def summary(self) -> str:
        lines = []
        for name in self.partition_names():
            lines.append(f"{name}: {self.size(name)} result(s)")
        if self.unmatched:
            lines.append(f"unmatched: {len(self.unmatched)} result(s)")
        return "\n".join(lines) if lines else "no results"


def split_results(
    results: List[DriftResult],
    routing_map: Dict[str, List[str]],
    default_partition: Optional[str] = None,
) -> SplitReport:
    """Split results into partitions according to a service-name routing map.

    Args:
        results: list of DriftResult objects to split.
        routing_map: mapping of partition_name -> list of service names.
        default_partition: if set, unmatched results go here instead of unmatched list.

    Returns:
        SplitReport with populated partitions.

    Raises:
        SplitterError: if results or routing_map are None, or routing_map is empty.
    """
    if results is None:
        raise SplitterError("results must not be None")
    if routing_map is None:
        raise SplitterError("routing_map must not be None")
    if not routing_map:
        raise SplitterError("routing_map must not be empty")

    # Validate partition names
    for name in routing_map:
        if not name or not name.strip():
            raise SplitterError("partition names must be non-empty strings")

    # Build reverse lookup: service -> partition
    service_to_partition: Dict[str, str] = {}
    for partition, services in routing_map.items():
        for svc in services:
            if svc in service_to_partition:
                raise SplitterError(
                    f"service '{svc}' appears in multiple partitions"
                )
            service_to_partition[svc] = partition

    partitions: Dict[str, List[DriftResult]] = {k: [] for k in routing_map}
    if default_partition and default_partition not in partitions:
        partitions[default_partition] = []
    unmatched: List[DriftResult] = []

    for result in results:
        target = service_to_partition.get(result.service)
        if target:
            partitions[target].append(result)
        elif default_partition:
            partitions[default_partition].append(result)
        else:
            unmatched.append(result)

    return SplitReport(partitions=partitions, unmatched=unmatched)
