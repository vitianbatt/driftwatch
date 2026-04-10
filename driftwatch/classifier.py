"""Classify drift results into named categories based on field patterns."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class ClassifierError(Exception):
    """Raised when classification configuration is invalid."""


@dataclass
class ClassificationRule:
    category: str
    pattern: str  # regex matched against field names

    def __post_init__(self) -> None:
        if not self.category or not self.category.strip():
            raise ClassifierError("category must be a non-empty string")
        if not self.pattern or not self.pattern.strip():
            raise ClassifierError("pattern must be a non-empty string")
        try:
            re.compile(self.pattern)
        except re.error as exc:
            raise ClassifierError(f"invalid regex pattern '{self.pattern}': {exc}") from exc


@dataclass
class ClassifiedResult:
    service: str
    categories: List[str] = field(default_factory=list)
    unclassified_fields: List[str] = field(default_factory=list)

    def has_category(self, category: str) -> bool:
        return category in self.categories

    def to_dict(self) -> Dict:
        return {
            "service": self.service,
            "categories": sorted(self.categories),
            "unclassified_fields": self.unclassified_fields,
        }


def classify_results(
    results: List[DriftResult],
    rules: List[ClassificationRule],
) -> List[ClassifiedResult]:
    """Classify each result's drifted fields using the provided rules."""
    if results is None:
        raise ClassifierError("results must not be None")
    if rules is None:
        raise ClassifierError("rules must not be None")

    classified: List[ClassifiedResult] = []
    for result in results:
        categories: List[str] = []
        unclassified: List[str] = []
        drift_fields = list(result.missing_keys) + list(result.extra_keys)
        for f in drift_fields:
            matched = False
            for rule in rules:
                if re.search(rule.pattern, f):
                    if rule.category not in categories:
                        categories.append(rule.category)
                    matched = True
            if not matched:
                unclassified.append(f)
        classified.append(
            ClassifiedResult(
                service=result.service,
                categories=categories,
                unclassified_fields=unclassified,
            )
        )
    return classified
