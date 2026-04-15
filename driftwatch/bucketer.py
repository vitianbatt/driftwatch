"""Bucketer: groups drift results into named time-based or count-based buckets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class BucketerError(Exception):
    """Raised when bucketing fails."""


@dataclass
class Bucket:
    name: str
    results: List[DriftResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def drift_count(self) -> int:
        return sum(1 for r in self.results if r.diffs)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "total": len(self),
            "drifted": self.drift_count(),
            "services": self.service_names(),
        }


@dataclass
class BucketedReport:
    buckets: Dict[str, Bucket] = field(default_factory=dict)

    def bucket_names(self) -> List[str]:
        return sorted(self.buckets.keys())

    def get(self, name: str) -> Optional[Bucket]:
        return self.buckets.get(name)

    def total(self) -> int:
        return sum(len(b) for b in self.buckets.values())

    def summary(self) -> str:
        if not self.buckets:
            return "No buckets."
        lines = []
        for name in self.bucket_names():
            b = self.buckets[name]
            lines.append(f"{name}: {len(b)} result(s), {b.drift_count()} drifted")
        return "\n".join(lines)


def bucket_results(
    results: List[DriftResult],
    bucket_map: Dict[str, List[str]],
) -> BucketedReport:
    """Assign results to named buckets based on service name lists.

    Args:
        results: List of DriftResult objects to bucket.
        bucket_map: Mapping of bucket name -> list of service names.

    Returns:
        BucketedReport with populated buckets.

    Raises:
        BucketerError: If results or bucket_map are None.
    """
    if results is None:
        raise BucketerError("results must not be None")
    if bucket_map is None:
        raise BucketerError("bucket_map must not be None")

    buckets: Dict[str, Bucket] = {name: Bucket(name=name) for name in bucket_map}

    for result in results:
        for bucket_name, services in bucket_map.items():
            if result.service in services:
                buckets[bucket_name].results.append(result)
                break

    return BucketedReport(buckets=buckets)
