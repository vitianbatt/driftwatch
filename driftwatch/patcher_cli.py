"""CLI helpers for the patcher module."""
from __future__ import annotations

import json
from typing import List

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.patcher import PatchReport, generate_patches


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON array of drift result dicts into DriftResult objects."""
    data = json.loads(raw)
    results = []
    for item in data:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d.get("expected"),
                actual=d.get("actual"),
            )
            for d in item.get("diffs", [])
        ]
        results.append(DriftResult(service=item["service"], diffs=diffs))
    return results


def report_to_json(report: PatchReport) -> str:
    """Serialise a PatchReport to a JSON string."""
    return json.dumps(
        {"total": report.total, "suggestions": [s.to_dict() for s in report.suggestions]},
        indent=2,
    )


def run_patcher(raw_json: str, output_format: str = "text") -> str:
    """Entry point used by the CLI: parse input, generate patches, return output."""
    results = results_from_json(raw_json)
    report = generate_patches(results)
    if output_format == "json":
        return report_to_json(report)
    return report.summary()
