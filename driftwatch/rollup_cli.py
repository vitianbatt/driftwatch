"""CLI helpers for the rollup command."""
from __future__ import annotations

import json
from typing import List

from driftwatch.comparator import DriftResult
from driftwatch.rollup import RollupReport, build_rollup, RollupError


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON array of drift result objects."""
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RollupError(f"Invalid JSON input: {exc}") from exc

    if not isinstance(items, list):
        raise RollupError("Expected a JSON array of drift results")

    results: List[DriftResult] = []
    for item in items:
        if "service" not in item:
            raise RollupError(f"Entry missing 'service' key: {item}")
        results.append(DriftResult(service=item["service"], diffs=item.get("diffs") or {}))
    return results


def report_to_json(report: RollupReport) -> str:
    """Serialise a RollupReport to a JSON string."""
    return json.dumps(
        {
            "total": report.total,
            "clean": report.clean,
            "drifted": report.drifted,
            "by_severity": report.by_severity,
            "services": report.services,
            "drifted_services": report.drifted_services,
            "has_any_drift": report.has_any_drift(),
        },
        indent=2,
    )


def run_rollup(raw_json: str, output_format: str = "text") -> str:
    """Parse raw JSON input, build rollup, return formatted output."""
    results = results_from_json(raw_json)
    report = build_rollup(results)
    if output_format == "json":
        return report_to_json(report)
    return report.summary()
