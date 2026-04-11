"""CLI helpers for the matcher module."""
from __future__ import annotations

import json
from typing import Any, Dict, List

import yaml

from driftwatch.comparator import DriftResult
from driftwatch.matcher import MatchRule, MatchReport, match_results


def rules_from_yaml(path: str) -> List[MatchRule]:
    """Load a list of :class:`MatchRule` objects from a YAML file."""
    with open(path) as fh:
        data = yaml.safe_load(fh)
    return [
        MatchRule(
            pattern=r["pattern"],
            use_regex=r.get("use_regex", False),
        )
        for r in data.get("rules", [])
    ]


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON list of drift result dicts."""
    items: List[Dict[str, Any]] = json.loads(raw)
    return [
        DriftResult(
            service=item["service"],
            drifted_fields=item.get("drifted_fields", []),
        )
        for item in items
    ]


def report_to_json(report: MatchReport) -> str:
    """Serialise a :class:`MatchReport` to a JSON string."""
    return json.dumps(
        {
            "matched": [
                {"service": r.service, "drifted_fields": r.drifted_fields}
                for r in report.matched
            ],
            "unmatched": [
                {"service": r.service, "drifted_fields": r.drifted_fields}
                for r in report.unmatched
            ],
            "summary": report.summary(),
        },
        indent=2,
    )


def run_matcher(
    results: List[DriftResult],
    rules: List[MatchRule],
    *,
    require_all: bool = False,
) -> str:
    """Run the matcher and return a JSON report string."""
    report = match_results(results, rules, require_all=require_all)
    return report_to_json(report)
