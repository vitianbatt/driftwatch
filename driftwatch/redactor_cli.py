"""CLI helpers for the redactor: load rules from YAML and run redaction."""

from __future__ import annotations

import json
from typing import List

import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.redactor import RedactRule, RedactedResult, RedactorError, redact_results


def rules_from_yaml(path: str) -> List[RedactRule]:
    """Load redaction rules from a YAML file.

    Expected format::

        rules:
          - pattern: "password"
          - pattern: "secret"
            mask: "[hidden]"
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except OSError as exc:
        raise RedactorError(f"cannot open rules file {path!r}: {exc}") from exc

    raw_rules = (data or {}).get("rules", [])
    rules: List[RedactRule] = []
    for item in raw_rules:
        kwargs = {"pattern": item["pattern"]}
        if "mask" in item:
            kwargs["mask"] = item["mask"]
        rules.append(RedactRule(**kwargs))
    return rules


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON array of drift results."""
    items = json.loads(raw)
    results: List[DriftResult] = []
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
        results.append(DriftResult(service=item["service"], diffs=diffs))
    return results


def report_to_json(redacted: List[RedactedResult]) -> str:
    """Serialise redacted results to a JSON string."""
    return json.dumps([r.to_dict() for r in redacted], indent=2)


def run_redactor(rules_path: str, results_json: str) -> str:
    """End-to-end helper used by the CLI entry-point."""
    rules = rules_from_yaml(rules_path)
    results = results_from_json(results_json)
    redacted = redact_results(results, rules)
    return report_to_json(redacted)
