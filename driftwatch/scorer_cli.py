"""CLI helpers for the scorer module."""
from __future__ import annotations

import json
from typing import Any

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScorerError, ScoredReport, score_results


def results_from_json(raw: str) -> list[DriftResult]:
    """Parse a JSON string into a list of DriftResult objects."""
    data: list[dict[str, Any]] = json.loads(raw)
    results: list[DriftResult] = []
    for item in data:
        diffs = [
            FieldDiff(field=d["field"], expected=d["expected"], actual=d["actual"])
            for d in item.get("diffs", [])
        ]
        results.append(DriftResult(service=item["service"], diffs=diffs))
    return results


def report_to_json(report: ScoredReport) -> str:
    """Serialise a ScoredReport to a JSON string."""
    payload = [
        {
            "service": sr.service,
            "score": sr.score,
            "priority": sr.priority,
            "drift_fields": sr.drift_fields,
        }
        for sr in report.results
    ]
    return json.dumps(payload, indent=2)


def run_scorer(raw_json: str) -> str:
    """End-to-end: parse results, score them, return JSON report."""
    results = results_from_json(raw_json)
    report = score_results(results)
    return report_to_json(report)
