"""Suppression rules for drift results — allows known/accepted drift to be silenced."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import fnmatch

from driftwatch.comparator import DriftResult


class SuppressionError(Exception):
    """Raised when a suppression rule is invalid or cannot be applied."""


@dataclass
class SuppressionRule:
    """A single rule that suppresses drift for a service/field pattern."""

    service: str  # glob pattern, e.g. "auth-*" or "payment-service"
    fields: List[str] = field(default_factory=list)  # glob patterns; empty means all fields
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.service or not self.service.strip():
            raise SuppressionError("SuppressionRule.service must be a non-empty string")

    def matches(self, service: str, field_key: str) -> bool:
        """Return True if this rule suppresses the given service+field combination."""
        if not fnmatch.fnmatch(service, self.service):
            return False
        if not self.fields:
            return True
        return any(fnmatch.fnmatch(field_key, pat) for pat in self.fields)


def apply_suppressions(
    results: List[DriftResult],
    rules: List[SuppressionRule],
) -> List[DriftResult]:
    """Return a new list of DriftResults with suppressed fields removed.

    A result whose every drifted field is suppressed will have an empty
    ``drifted_fields`` list (i.e. it will appear clean after suppression).
    Results with no drift are passed through unchanged.
    """
    if not rules:
        return list(results)

    suppressed: List[DriftResult] = []
    for result in results:
        if not result.drifted_fields:
            suppressed.append(result)
            continue

        remaining = [
            f for f in result.drifted_fields
            if not any(rule.matches(result.service, f) for rule in rules)
        ]
        suppressed.append(
            DriftResult(service=result.service, drifted_fields=remaining)
        )
    return suppressed


def load_rules_from_dicts(raw: List[dict]) -> List[SuppressionRule]:
    """Parse a list of plain dicts (e.g. loaded from YAML) into SuppressionRule objects."""
    rules: List[SuppressionRule] = []
    for idx, item in enumerate(raw):
        if "service" not in item:
            raise SuppressionError(f"Suppression rule at index {idx} is missing 'service' key")
        rules.append(
            SuppressionRule(
                service=item["service"],
                fields=item.get("fields", []),
                reason=item.get("reason"),
            )
        )
    return rules
