"""linker_cli.py – CLI helpers for the linker module."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.linker import DependencyMap, LinkerError, link_results


def dep_map_from_yaml(path: str) -> DependencyMap:
    """Load a DependencyMap from a YAML file."""
    p = Path(path)
    if not p.exists():
        raise LinkerError(f"dependency map file not found: {path}")
    raw = yaml.safe_load(p.read_text())
    if "deps" not in raw:
        raise LinkerError("dependency map YAML must contain a 'deps' key")
    return DependencyMap(deps=raw["deps"])


def results_from_json(data: List[dict]) -> List[DriftResult]:
    """Deserialise a list of plain dicts into DriftResult objects."""
    out: List[DriftResult] = []
    for item in data:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d.get("expected"),
                actual=d.get("actual"),
                kind=d.get("kind", "changed"),
            )
            for d in item.get("diffs", [])
        ]
        out.append(DriftResult(service=item["service"], diffs=diffs))
    return out


def report_to_json(linked) -> str:
    """Serialise linked results to a JSON string."""
    return json.dumps([lr.to_dict() for lr in linked], indent=2)


def run_linker(results_path: str, dep_map_path: str) -> str:
    """Load results and dep map from disk, run linking, return JSON report."""
    raw = json.loads(Path(results_path).read_text())
    results = results_from_json(raw)
    dep_map = dep_map_from_yaml(dep_map_path)
    linked = link_results(results, dep_map)
    return report_to_json(linked)
