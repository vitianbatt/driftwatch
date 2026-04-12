"""CLI helpers for the pruner module."""
from __future__ import annotations

import json
from typing import Any

from driftwatch.pruner import PruneConfig, PrunedReport, prune
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def config_from_dict(raw: dict[str, Any]) -> PruneConfig:
    """Build a PruneConfig from a plain dict (e.g. parsed YAML)."""
    return PruneConfig(
        max_age_days=raw.get("max_age_days"),
        max_results=raw.get("max_results"),
        drop_clean=bool(raw.get("drop_clean", False)),
    )


def results_from_json(text: str) -> list[DriftResult]:
    """Deserialise a JSON array of drift results."""
    items = json.loads(text)
    out: list[DriftResult] = []
    for item in items:
        diffs = [
            FieldDiff(
                field=d["field"],
                kind=d["kind"],
                expected=d.get("expected"),
                actual=d.get("actual"),
            )
            for d in item.get("diffs", [])
        ]
        out.append(DriftResult(service=item["service"], diffs=diffs))
    return out


def report_to_json(report: PrunedReport) -> str:
    """Serialise a PrunedReport to a JSON string."""
    entries = [
        {
            "service": r.service,
            "diffs": [
                {
                    "field": d.field,
                    "kind": d.kind,
                    "expected": d.expected,
                    "actual": d.actual,
                }
                for d in r.diffs
            ],
        }
        for r in report.kept
    ]
    return json.dumps(
        {
            "kept": entries,
            "total": report.total,
            "pruned_count": report.pruned_count,
        },
        indent=2,
    )


def run_pruner(config: PruneConfig, results: list[DriftResult]) -> PrunedReport:
    """Run the pruner and return a PrunedReport."""
    return prune(config, results)
