"""Partitioner: splits DriftResults into named partitions by environment tag."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class PartitionerError(Exception):
    """Raised when partitioning fails."""


@dataclass
class PartitionConfig:
    env_field: str = "environment"
    default_partition: str = "unknown"

    def __post_init__(self) -> None:
        if not self.env_field or not self.env_field.strip():
            raise PartitionerError("env_field must be a non-empty string")
        if not self.default_partition or not self.default_partition.strip():
            raise PartitionerError("default_partition must be a non-empty string")


@dataclass
class PartitionedReport:
    partitions: Dict[str, List[DriftResult]] = field(default_factory=dict)

    def partition_names(self) -> List[str]:
        return sorted(self.partitions.keys())

    def size(self, name: str) -> int:
        return len(self.partitions.get(name, []))

    def total(self) -> int:
        return sum(len(v) for v in self.partitions.values())

    def summary(self) -> str:
        if not self.partitions:
            return "No partitions."
        lines = []
        for name in self.partition_names():
            count = self.size(name)
            lines.append(f"{name}: {count} result(s)")
        return "\n".join(lines)


def _extract_env(result: DriftResult, config: PartitionConfig) -> str:
    """Pull the environment value from the spec dict, falling back to default."""
    spec = getattr(result, "spec", {}) or {}
    value = spec.get(config.env_field)
    if not value or not str(value).strip():
        return config.default_partition
    return str(value).strip()


def partition_results(
    results: Optional[List[DriftResult]],
    config: Optional[PartitionConfig] = None,
) -> PartitionedReport:
    """Partition results by environment field found in each result's spec."""
    if results is None:
        raise PartitionerError("results must not be None")
    if config is None:
        config = PartitionConfig()

    report: Dict[str, List[DriftResult]] = {}
    for result in results:
        env = _extract_env(result, config)
        report.setdefault(env, []).append(result)

    return PartitionedReport(partitions=report)
