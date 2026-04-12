"""Highlighter: mark drift results that match field patterns for emphasis."""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class HighlighterError(Exception):
    """Raised when highlighter configuration or input is invalid."""


@dataclass
class HighlightRule:
    """A rule that marks fields matching a glob pattern."""

    pattern: str
    label: str = "highlighted"

    def __post_init__(self) -> None:
        if not self.pattern or not self.pattern.strip():
            raise HighlighterError("pattern must be a non-empty string")
        if not self.label or not self.label.strip():
            raise HighlighterError("label must be a non-empty string")
        self.pattern = self.pattern.strip()
        self.label = self.label.strip()

    def matches_field(self, field_name: str) -> bool:
        return fnmatch.fnmatch(field_name, self.pattern)


@dataclass
class HighlightedResult:
    """A drift result annotated with per-field highlight labels."""

    service: str
    diffs: List[FieldDiff]
    highlights: dict = field(default_factory=dict)  # field_name -> label

    def has_drift(self) -> bool:
        return bool(self.diffs)

    def is_highlighted(self, field_name: str) -> bool:
        return field_name in self.highlights

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "highlights": self.highlights,
            "drift_fields": [d.field for d in self.diffs],
        }


def highlight_results(
    results: List[DriftResult],
    rules: List[HighlightRule],
) -> List[HighlightedResult]:
    """Apply highlight rules to a list of drift results."""
    if results is None:
        raise HighlighterError("results must not be None")
    if rules is None:
        raise HighlighterError("rules must not be None")

    highlighted: List[HighlightedResult] = []
    for result in results:
        hit: dict = {}
        for diff in result.diffs:
            for rule in rules:
                if rule.matches_field(diff.field):
                    hit[diff.field] = rule.label
                    break
        highlighted.append(
            HighlightedResult(
                service=result.service,
                diffs=result.diffs,
                highlights=hit,
            )
        )
    return highlighted
