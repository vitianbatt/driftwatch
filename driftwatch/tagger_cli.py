"""CLI helpers for the tagger module."""
from __future__ import annotations

import json
from typing import Any

import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.tagging import TaggedResult, tag_results, filter_by_tag


def tag_map_from_yaml(path: str) -> dict[str, list[str]]:
    """Load a tag map from a YAML file."""
    with open(path) as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Tag map must be a YAML mapping, got {type(data).__name__}")
    return {str(k): list(v) for k, v in data.items()}


def results_from_json(raw: str) -> list[DriftResult]:
    """Parse a JSON array of drift results."""
    items: list[dict[str, Any]] = json.loads(raw)
    out: list[DriftResult] = []
    for item in items:
        diffs = [
            FieldDiff(field=d["field"], expected=d["expected"], actual=d["actual"])
            for d in item.get("diffs", [])
        ]
        out.append(DriftResult(service=item["service"], diffs=diffs))
    return out


def report_to_json(tagged: list[TaggedResult]) -> str:
    """Serialise tagged results to a JSON string."""
    rows = [
        {
            "service": t.result.service,
            "tags": sorted(t.tags),
            "drift_fields": [d.field for d in t.result.diffs],
        }
        for t in tagged
    ]
    return json.dumps(rows, indent=2)


def run_tagger(
    results_json: str,
    tag_map_path: str,
    filter_tag: str | None = None,
) -> str:
    """End-to-end helper: load tag map, apply tags, optionally filter."""
    tag_map = tag_map_from_yaml(tag_map_path)
    results = results_from_json(results_json)
    tagged = tag_results(results, tag_map)
    if filter_tag is not None:
        tagged = filter_by_tag(tagged, filter_tag)
    return report_to_json(tagged)
