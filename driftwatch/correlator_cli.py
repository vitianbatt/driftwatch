"""CLI helpers for the correlator module."""
from __future__ import annotations

import json
from typing import List

from driftwatch.comparator import DriftResult
from driftwatch.correlator import CorrelationReport, correlate


def results_from_json(raw: str) -> List[DriftResult]:
    """Parse a JSON array of drift result objects into DriftResult instances."""
    records = json.loads(raw)
    if not isinstance(records, list):
        raise ValueError("Expected a JSON array of drift result objects")
    out: List[DriftResult] = []
    for rec in records:
        out.append(
            DriftResult(
                service=rec["service"],
                drifted_fields=rec.get("drifted_fields") or [],
            )
        )
    return out


def report_to_json(report: CorrelationReport) -> str:
    """Serialise a CorrelationReport to a JSON string."""
    return json.dumps(
        {
            "total_groups": report.total_groups(),
            "groups": [g.to_dict() for g in report.groups],
        },
        indent=2,
    )


def run_correlator(raw_json: str) -> str:
    """End-to-end helper: parse JSON input, correlate, return JSON report."""
    results = results_from_json(raw_json)
    report = correlate(results)
    return report_to_json(report)
