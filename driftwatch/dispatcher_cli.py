"""CLI helpers for the dispatcher: load rules from JSON and run dispatch."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from driftwatch.comparator import DriftResult
from driftwatch.dispatcher import DispatchReport, DispatchRule, dispatch


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON array of result objects into DriftResult instances."""
    items: List[Dict[str, Any]] = json.loads(raw)
    out = []
    for item in items:
        out.append(DriftResult(service=item["service"], diffs=item.get("diffs", [])))
    return out


def report_to_json(report: DispatchReport) -> str:
    """Serialise a DispatchReport to a compact JSON string."""
    return json.dumps(
        {
            "dispatched": report.dispatched,
            "skipped": report.skipped,
            "total_dispatched": report.total_dispatched(),
        },
        indent=2,
    )


def run_dispatcher(
    results_json: str,
    rules: List[DispatchRule],
    *,
    output_format: str = "text",
) -> str:
    """Run dispatch and return a text summary or JSON report."""
    results = results_from_json(results_json)
    report = dispatch(results, rules)
    if output_format == "json":
        return report_to_json(report)
    return report.summary()
