"""CLI helpers for the drift scorer module."""
from __future__ import annotations

import json
from typing import Any

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredReport, score_results


def results_from_json(raw: list[dict[str, Any]]) -> list[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
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


def report_to_json(report: ScoredReport) -> str:
    """Serialise a ScoredReport to a JSON string."""
    return json.dumps(
        {"results": [r.to_dict() for r in report.results]},
        indent=2,
    )


def run_scorer(raw_results: list[dict[str, Any]], weights: dict[str, float] | None = None) -> str:
    """End-to-end helper: parse raw dicts, score them, return JSON."""
    results = results_from_json(raw_results)
    report = score_results(results, weights=weights or {})
    return report_to_json(report)
