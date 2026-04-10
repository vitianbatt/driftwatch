"""CLI helpers for the classifier module."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import yaml

from driftwatch.classifier import ClassificationRule, ClassifiedResult, classify_results
from driftwatch.comparator import DriftResult


def rules_from_yaml(path: str) -> List[ClassificationRule]:
    """Load classification rules from a YAML file."""
    with open(path, "r") as fh:
        data = yaml.safe_load(fh)
    raw = data.get("rules", [])
    return [
        ClassificationRule(category=r["category"], pattern=r["pattern"])
        for r in raw
    ]


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON list of drift result dicts into DriftResult objects."""
    items: List[Dict[str, Any]] = json.loads(raw)
    return [
        DriftResult(
            service=item["service"],
            missing_keys=item.get("missing_keys", []),
            extra_keys=item.get("extra_keys", []),
        )
        for item in items
    ]


def report_to_json(classified: List[ClassifiedResult]) -> str:
    """Serialise classified results to a JSON string."""
    return json.dumps([r.to_dict() for r in classified], indent=2)


def run_classifier(results_json: str, rules_path: str) -> str:
    """End-to-end: load rules, classify results, return JSON report."""
    rules = rules_from_yaml(rules_path)
    results = results_from_json(results_json)
    classified = classify_results(results, rules)
    return report_to_json(classified)
