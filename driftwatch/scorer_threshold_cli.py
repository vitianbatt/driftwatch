"""scorer_threshold_cli.py – CLI helpers for the scorer threshold filter."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_threshold import (
    ScorerThresholdError,
    ThresholdConfig,
    ThresholdedReport,
    apply_threshold,
)


def results_from_json(data: List[Dict[str, Any]]) -> ScoredReport:
    """Parse a list of raw dicts into a ScoredReport."""
    results = []
    for item in data:
        diffs = [
            FieldDiff(
                field=d["field"],
                kind=d["kind"],
                expected=d.get("expected", ""),
                actual=d.get("actual", ""),
            )
            for d in item.get("diffs", [])
        ]
        dr = DriftResult(service=item["service"], diffs=diffs)
        results.append(ScoredResult(result=dr, score=float(item.get("score", 0.0))))
    return ScoredReport(results=results)


def report_to_json(report: ThresholdedReport) -> str:
    """Serialise a ThresholdedReport to a JSON string."""
    payload = {
        "threshold": report.config.min_score,
        "include_clean": report.config.include_clean,
        "total_kept": report.total_kept,
        "total_dropped": report.total_dropped,
        "kept": [
            {
                "service": r.result.service,
                "score": r.score,
                "diffs": [
                    {"field": d.field, "kind": d.kind}
                    for d in r.result.diffs
                ],
            }
            for r in report.kept
        ],
    }
    return json.dumps(payload, indent=2)


def run_threshold(raw: List[Dict[str, Any]], min_score: float = 0.0,
                  include_clean: bool = False) -> str:
    """End-to-end helper: parse → filter → serialise."""
    try:
        config = ThresholdConfig(min_score=min_score, include_clean=include_clean)
    except ScorerThresholdError as exc:
        raise ScorerThresholdError(f"Invalid threshold config: {exc}") from exc
    scored = results_from_json(raw)
    report = apply_threshold(scored, config)
    return report_to_json(report)
