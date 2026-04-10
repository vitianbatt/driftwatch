"""CLI helpers for the grouper module.

Used by ``driftwatch/cli.py`` to expose the ``group`` sub-command.
"""
from __future__ import annotations

import json
from typing import Dict, List

from driftwatch.comparator import DriftResult
from driftwatch.grouper import GroupBy, GroupedReport, GrouperError, group_results


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON string produced by a previous ``check`` run.

    Raises
    ------
    GrouperError
        If *raw* is not valid JSON or is not a JSON array.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GrouperError(f"Invalid JSON input: {exc}") from exc
    if not isinstance(data, list):
        raise GrouperError("Expected a JSON array of drift results")
    out: List[DriftResult] = []
    for item in data:
        diffs = [str(d) for d in item.get("diffs", [])]
        out.append(DriftResult(service=item["service"], diffs=diffs))
    return out


def report_to_json(report: GroupedReport) -> str:
    """Serialise *report* to a JSON string."""
    payload = {
        "dimension": report.dimension,
        "total": report.total(),
        "groups": {
            name: len(items) for name, items in report.groups.items()
        },
    }
    return json.dumps(payload, indent=2)


def run_grouper(
    results_json: str,
    by: str,
    tag_map: Dict[str, str] | None = None,
    output_json: bool = False,
) -> str:
    """Entry point called by the CLI.

    Parameters
    ----------
    results_json:
        JSON string of drift results.
    by:
        Grouping dimension name (``"service"``, ``"severity"``, ``"tag"``).
    tag_map:
        Required when *by* is ``"tag"``.
    output_json:
        When ``True`` return JSON; otherwise return a human-readable summary.
    """
    try:
        dimension = GroupBy(by)
    except ValueError:
        valid = ", ".join(g.value for g in GroupBy)
        raise GrouperError(f"Unknown dimension '{by}'. Valid options: {valid}")

    if dimension is GroupBy.TAG and not tag_map:
        raise GrouperError(
            "A tag_map must be provided when grouping by 'tag'"
        )

    results = results_from_json(results_json)
    report = group_results(results, dimension, tag_map=tag_map)

    if output_json:
        return report_to_json(report)
    return report.summary()
