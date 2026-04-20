"""CLI helpers for the remediation planner."""
from __future__ import annotations

import json
from typing import Any

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.planner import RemediationPlan, build_plan
from driftwatch.scorer import ScoredResult, score_results


def results_from_json(raw: list[dict[str, Any]]) -> list[DriftResult]:
    """Deserialise plain dicts into DriftResult objects."""
    out: list[DriftResult] = []
    for item in raw:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d.get("expected"),
                actual=d.get("actual"),
                diff_type=d.get("diff_type", "changed"),
            )
            for d in item.get("diffs", [])
        ]
        out.append(DriftResult(service=item["service"], diffs=diffs))
    return out


def plan_to_json(plan: RemediationPlan) -> str:
    """Serialise a RemediationPlan to a JSON string."""
    return json.dumps(
        {
            "total": plan.total,
            "items": [i.to_dict() for i in plan.items],
        },
        indent=2,
    )


def run_planner(
    raw_results: list[dict[str, Any]],
    weights: dict[str, float] | None = None,
) -> str:
    """End-to-end helper: parse → score → plan → serialise."""
    drift_results = results_from_json(raw_results)
    scored_report = score_results(drift_results, weights=weights)
    plan = build_plan(scored_report.results)
    return plan_to_json(plan)
