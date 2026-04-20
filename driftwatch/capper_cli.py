"""CLI helpers for the capper module."""

from __future__ import annotations

import json
from typing import Any

from driftwatch.capper import CapConfig, CappedResult, cap_results
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def results_from_json(raw: list[dict[str, Any]]) -> list[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
    results: list[DriftResult] = []
    for entry in raw:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d["expected"],
                actual=d["actual"],
                kind=d["kind"],
            )
            for d in entry.get("diffs", [])
        ]
        results.append(DriftResult(service=entry["service"], diffs=diffs))
    return results


def report_to_json(capped: list[CappedResult]) -> str:
    """Serialise capped results to a JSON string."""
    records = [
        {
            "service": r.service,
            "has_drift": r.has_drift(),
            "shown": len(r.diffs),
            "total": r.total_diffs,
            "capped": r.was_capped,
            "diffs": [
                {
                    "field": d.field,
                    "expected": d.expected,
                    "actual": d.actual,
                    "kind": d.kind,
                }
                for d in r.diffs
            ],
        }
        for r in capped
    ]
    return json.dumps(records, indent=2)


def run_capper(
    raw: list[dict[str, Any]],
    max_diffs: int = 5,
) -> str:
    """End-to-end: parse raw dicts, apply cap, return JSON string."""
    config = CapConfig(max_diffs=max_diffs)
    results = results_from_json(raw)
    capped = cap_results(results, config)
    return report_to_json(capped)
