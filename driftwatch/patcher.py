"""Patcher: generates remediation patch suggestions from drift results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class PatcherError(Exception):
    """Raised when patch generation fails."""


@dataclass
class PatchSuggestion:
    service: str
    field: str
    action: str          # "set", "remove", or "update"
    expected: Optional[object] = None
    actual: Optional[object] = None

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "field": self.field,
            "action": self.action,
            "expected": self.expected,
            "actual": self.actual,
        }

    def describe(self) -> str:
        if self.action == "set":
            return f"[{self.service}] SET '{self.field}' = {self.expected!r}  (missing in live)"
        if self.action == "remove":
            return f"[{self.service}] REMOVE '{self.field}'  (extra in live, value={self.actual!r})"
        return (
            f"[{self.service}] UPDATE '{self.field}': "
            f"{self.actual!r} -> {self.expected!r}"
        )


@dataclass
class PatchReport:
    suggestions: List[PatchSuggestion] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.suggestions)

    def has_suggestions(self) -> bool:
        return bool(self.suggestions)

    def summary(self) -> str:
        if not self.suggestions:
            return "No patches required."
        lines = [f"{self.total} patch suggestion(s):"]
        lines.extend(s.describe() for s in self.suggestions)
        return "\n".join(lines)


def _action_for_diff(d: FieldDiff) -> str:
    if d.expected is None:
        return "remove"
    if d.actual is None:
        return "set"
    return "update"


def generate_patches(results: List[DriftResult]) -> PatchReport:
    """Build a PatchReport from a list of DriftResults."""
    if results is None:
        raise PatcherError("results must not be None")

    suggestions: List[PatchSuggestion] = []
    for result in results:
        for diff in result.diffs:
            suggestions.append(
                PatchSuggestion(
                    service=result.service,
                    field=diff.field,
                    action=_action_for_diff(diff),
                    expected=diff.expected,
                    actual=diff.actual,
                )
            )
    return PatchReport(suggestions=suggestions)
