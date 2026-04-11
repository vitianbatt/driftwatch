"""CLI helpers for the partitioner module."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.partitioner import PartitionConfig, PartitionedReport, partition_results


def results_from_json(data: List[Dict[str, Any]]) -> List[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
    out = []
    for entry in data:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d["expected"],
                actual=d["actual"],
                kind=d.get("kind", "changed"),
            )
            for d in (entry.get("diffs") or [])
        ]
        out.append(
            DriftResult(
                service=entry["service"],
                diffs=diffs,
                spec=entry.get("spec", {}),
                live=entry.get("live", {}),
            )
        )
    return out


def report_to_json(report: PartitionedReport) -> str:
    """Serialise a PartitionedReport to a JSON string."""
    payload: Dict[str, Any] = {
        "total": report.total(),
        "partitions": {
            name: [r.service for r in items]
            for name, items in report.partitions.items()
        },
    }
    return json.dumps(payload, indent=2)


def run_partitioner(
    raw_results: List[Dict[str, Any]],
    env_field: str = "environment",
    default_partition: str = "unknown",
) -> str:
    """End-to-end helper: deserialise, partition, and return JSON string."""
    results = results_from_json(raw_results)
    config = PartitionConfig(env_field=env_field, default_partition=default_partition)
    report = partition_results(results, config=config)
    return report_to_json(report)
