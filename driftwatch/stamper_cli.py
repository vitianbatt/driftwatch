"""stamper_cli.py — CLI helpers for the stamper module."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.stamper import StampReport, stamp_results


def results_from_json(raw: List[Dict[str, Any]]) -> List[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
    out: List[DriftResult] = []
    for item in raw:
        out.append(
            DriftResult(
                service=item["service"],
                drifted_fields=item.get("drifted_fields", []),
            )
        )
    return out


def report_to_json(report: StampReport) -> str:
    """Serialise a StampReport to a JSON string."""
    return json.dumps(
        {
            "summary": report.summary(),
            "results": [r.to_dict() for r in report.results],
        },
        indent=2,
    )


def run_stamper(
    raw_results: List[Dict[str, Any]],
    stamp: str,
    source: Optional[str] = None,
) -> str:
    """End-to-end helper: parse *raw_results*, stamp them, return JSON."""
    results = results_from_json(raw_results)
    report = stamp_results(results, stamp=stamp, source=source)
    return report_to_json(report)
