"""CLI helpers for the resolver module."""
from __future__ import annotations

import json
from typing import Any, Dict, List

import yaml

from driftwatch.comparator import DriftResult
from driftwatch.resolver import OwnerMap, ResolvedResult, resolve_results, unowned


def owner_map_from_yaml(path: str) -> OwnerMap:
    """Load an OwnerMap from a YAML file with a top-level 'owners' dict."""
    with open(path) as fh:
        data = yaml.safe_load(fh)
    mappings: Dict[str, str] = data.get("owners", {})
    return OwnerMap(mappings)


def results_from_json(raw: str) -> List[DriftResult]:
    """Deserialise a JSON array of drift result objects."""
    items: List[Dict[str, Any]] = json.loads(raw)
    out: List[DriftResult] = []
    for item in items:
        out.append(
            DriftResult(
                service=item["service"],
                drift_fields=item.get("drift_fields", []),
            )
        )
    return out


def report_to_json(resolved: List[ResolvedResult]) -> str:
    """Serialise resolved results to a JSON array."""
    return json.dumps([r.to_dict() for r in resolved], indent=2)


def run_resolver(results_json: str, owner_map_path: str, show_unowned: bool = False) -> str:
    """End-to-end: resolve results and return JSON report.

    If *show_unowned* is True, only unowned results are included.
    """
    results = results_from_json(results_json)
    owner_map = owner_map_from_yaml(owner_map_path)
    resolved = resolve_results(results, owner_map)
    if show_unowned:
        resolved = unowned(resolved)
    return report_to_json(resolved)
