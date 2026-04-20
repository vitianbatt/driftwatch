"""CLI helpers for the scorer module."""
from __future__ import annotations

import json
from typing import Any

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredReport, ScorerError, score_results


def results_from_json(raw: list[dict[str, Any]]) -> list[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
    results: list[DriftResult] = []
    for item in raw:
        diffs = [
            FieldDiff(
                field=d["field"],
                diff_type=d["diff_type"],
                expected=d.get("expected"),
                actual=d.get("actual"),
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
        {
            "average_score": report.average,
            "results": [r.to_dict() for r in report.results],
        },
        indent=2,
    )


def run_scorer(raw_results: list[dict[str, Any]]) -> ScoredReport:
    """Parse raw dicts and return a scored report."""
    results = results_from_json(raw_results)
    if results is None:
        raise ScorerError("No results provided")
    return score_results(results)
