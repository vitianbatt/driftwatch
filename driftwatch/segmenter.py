"""Segment drift results into named buckets based on field-name patterns."""
from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class SegmenterError(Exception):
    """Raised when segmentation configuration or input is invalid."""


@dataclass
class SegmentRule:
    name: str
    pattern: str  # fnmatch-style glob matched against drift field names

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise SegmenterError("SegmentRule name must not be empty")
        if not self.pattern or not self.pattern.strip():
            raise SegmenterError("SegmentRule pattern must not be empty")
        self.name = self.name.strip()
        self.pattern = self.pattern.strip()

    def matches_field(self, field_name: str) -> bool:
        return fnmatch.fnmatch(field_name, self.pattern)


@dataclass
class SegmentedReport:
    segments: Dict[str, List[DriftResult]] = field(default_factory=dict)
    unmatched: List[DriftResult] = field(default_factory=list)

    def segment_names(self) -> List[str]:
        return sorted(self.segments.keys())

    def size(self, segment_name: str) -> int:
        return len(self.segments.get(segment_name, []))

    def total(self) -> int:
        return sum(len(v) for v in self.segments.values()) + len(self.unmatched)


def segment_results(
    results: Optional[List[DriftResult]],
    rules: Optional[List[SegmentRule]],
) -> SegmentedReport:
    """Assign each drifted result to the first rule whose pattern matches any
    of its drifted field names.  Results with no drift fields, or whose fields
    match no rule, go into *unmatched*."""
    if results is None:
        raise SegmenterError("results must not be None")
    if rules is None:
        raise SegmenterError("rules must not be None")

    report = SegmentedReport(segments={r.name: [] for r in rules})

    for result in results:
        drifted_fields = result.drifted_fields if result.drifted_fields else []
        placed = False
        for rule in rules:
            if any(rule.matches_field(f) for f in drifted_fields):
                report.segments[rule.name].append(result)
                placed = True
                break
        if not placed:
            report.unmatched.append(result)

    return report
