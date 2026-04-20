"""CLI helpers for the drift scorer module."""
from __future__ import annotations

import json
from typing import Any

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredReport, ScoredResult, score_results


def results_from_json(raw: list[dict[str, Any]]) -> list[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
    out: list[DriftResult] = []
    for item in raw:
        diffs = [
            FieldDiff(field=d["field"], kind=d["kind"],
                      expected=d.get("expected"), actual=d.get("actual"))
            for d in item.get("diffs", [])
        ]
        out.append(DriftResult(service=item["service"], diffs=diffs))
    return out


def report_to_json(report: ScoredReport) -> str:
    """Serialise a ScoredReport to a JSON string."""
    return json.dumps(
        [r.to_dict() for r in report.results],
        indent=2,
    )


def run_scorer(raw_results: list[dict[str, Any]]) -> ScoredReport:
    """End-to-end helper: parse raw dicts, score them, return the report."""
    results = results_from_json(raw_results)
    return score_results(results)
