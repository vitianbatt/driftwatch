"""CLI helper for running baseline comparisons from JSON input."""

from __future__ import annotations

import json
from typing import Any

from driftwatch.baseline_comparator import (
    BaselineCompareError,
    BaselineDriftReport,
    has_drift,
    summary,
)
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def results_from_json(raw: list[dict[str, Any]]) -> list[DriftResult]:
    """Parse a list of plain dicts into DriftResult objects."""
    results: list[DriftResult] = []
    for item in raw:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d.get("expected"),
                actual=d.get("actual"),
                diff_type=d.get("diff_type", "changed"),
            )
            for d in item.get("diffs", [])
        ]
        results.append(
            DriftResult(
                service=item["service"],
                diffs=diffs,
            )
        )
    return results


def report_to_json(report: BaselineDriftReport) -> str:
    """Serialise a BaselineDriftReport to a JSON string."""
    payload = {
        "has_drift": has_drift(report),
        "summary": summary(report),
        "services": [
            {
                "service": entry.service,
                "has_drift": entry.has_drift,
                "drift_fields": entry.drift_fields,
            }
            for entry in report.entries
        ],
    }
    return json.dumps(payload, indent=2)


def run_comparer(raw_results: list[dict[str, Any]], baseline: dict[str, Any]) -> str:
    """End-to-end: parse results, compare against baseline, return JSON report."""
    from driftwatch.baseline_comparator import compare_to_baseline

    results = results_from_json(raw_results)
    report = compare_to_baseline(results, baseline)
    return report_to_json(report)
