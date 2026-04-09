"""CLI helpers for suppression: load rules from a YAML file and report suppressed drift."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import yaml

from driftwatch.comparator import DriftResult
from driftwatch.suppressor import SuppressionError, SuppressionRule, apply_suppressions, load_rules_from_dicts


def load_suppression_file(path: str) -> List[SuppressionRule]:
    """Load suppression rules from a YAML file at *path*.

    The file must contain a top-level ``rules`` list.
    Raises ``SuppressionError`` if the file is missing or malformed.
    """
    p = Path(path)
    if not p.exists():
        raise SuppressionError(f"Suppression file not found: {path}")
    try:
        data = yaml.safe_load(p.read_text())
    except yaml.YAMLError as exc:
        raise SuppressionError(f"Failed to parse suppression file: {exc}") from exc
    if not isinstance(data, dict) or "rules" not in data:
        raise SuppressionError("Suppression file must contain a top-level 'rules' key")
    return load_rules_from_dicts(data["rules"])


def apply_and_summarise(
    results: List[DriftResult],
    rules: List[SuppressionRule],
    *,
    verbose: bool = False,
) -> str:
    """Apply suppression rules and return a human-readable summary string."""
    after = apply_suppressions(results, rules)

    total = len(results)
    drifted_before = sum(1 for r in results if r.drifted_fields)
    drifted_after = sum(1 for r in after if r.drifted_fields)
    suppressed_count = drifted_before - drifted_after

    lines: List[str] = [
        f"Suppression summary: {suppressed_count} service(s) silenced "
        f"({drifted_before} → {drifted_after} drifted out of {total})",
    ]

    if verbose:
        for before, result in zip(results, after):
            removed = set(before.drifted_fields) - set(result.drifted_fields)
            if removed:
                fields_str = ", ".join(sorted(removed))
                lines.append(f"  [{before.service}] suppressed fields: {fields_str}")

    return "\n".join(lines)
