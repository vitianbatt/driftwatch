"""CLI helper for scorer_merger: merge two JSONL scored-result streams."""
from __future__ import annotations

import json
from typing import List

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_merger import MergedScoredReport, merge_scored_reports


def _parse_diff(raw: dict) -> FieldDiff:
    return FieldDiff(
        field=raw["field"],
        kind=raw.get("kind", "missing"),
        expected=raw.get("expected"),
        actual=raw.get("actual"),
    )


def results_from_json(raw_list: list) -> ScoredReport:
    """Parse a list of dicts into a ScoredReport."""
    results: List[ScoredResult] = []
    for item in raw_list:
        diffs = [_parse_diff(d) for d in item.get("diffs", [])]
        drift = DriftResult(service=item["service"], diffs=diffs)
        results.append(
            ScoredResult(
                result=drift,
                score=float(item.get("score", 0.0)),
                drifted_fields=item.get("drifted_fields", []),
            )
        )
    return ScoredReport(results=results)


def report_to_json(report: MergedScoredReport) -> str:
    """Serialise a MergedScoredReport to a JSON string."""
    payload = {
        "total": report.total(),
        "conflict_count": report.conflict_count,
        "average_score": round(report.average_score(), 4),
        "results": [r.to_dict() for r in report.results],
    }
    return json.dumps(payload, indent=2)


def run_merger(primary_raw: list, secondary_raw: list) -> str:
    """Entry point: merge two raw result lists and return JSON string."""
    primary = results_from_json(primary_raw)
    secondary = results_from_json(secondary_raw)
    report = merge_scored_reports(primary, secondary)
    return report_to_json(report)
