"""Normalize drift result field keys before comparison or reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from driftwatch.comparator import DriftResult


class NormalizerError(Exception):
    """Raised when normalization cannot be applied."""


@dataclass
class NormalizationMap:
    """Mapping of raw field names to canonical names."""

    rules: Dict[str, str]

    def __post_init__(self) -> None:
        if not isinstance(self.rules, dict):
            raise NormalizerError("rules must be a dict")
        for raw, canonical in self.rules.items():
            if not raw or not raw.strip():
                raise NormalizerError("raw key must be a non-empty string")
            if not canonical or not canonical.strip():
                raise NormalizerError("canonical key must be a non-empty string")

    def translate(self, key: str) -> str:
        """Return canonical name for *key*, or *key* unchanged if not mapped."""
        return self.rules.get(key, key)


@dataclass
class NormalizedResult:
    """A DriftResult whose drift-field keys have been normalized."""

    service: str
    diffs: List  # list of FieldDiff (keys already translated)
    original: DriftResult

    def has_drift(self) -> bool:
        return bool(self.diffs)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "diff_count": len(self.diffs),
        }


def normalize_results(
    results: List[DriftResult],
    norm_map: NormalizationMap,
) -> List[NormalizedResult]:
    """Apply *norm_map* to the field names in every result."""
    if results is None:
        raise NormalizerError("results must not be None")
    if norm_map is None:
        raise NormalizerError("norm_map must not be None")

    normalized: List[NormalizedResult] = []
    for result in results:
        translated_diffs = []
        for diff in result.diffs:
            # FieldDiff is a dataclass with a .field attribute
            new_diff = diff.__class__(
                field=norm_map.translate(diff.field),
                kind=diff.kind,
                expected=diff.expected,
                actual=diff.actual,
            )
            translated_diffs.append(new_diff)
        normalized.append(
            NormalizedResult(
                service=result.service,
                diffs=translated_diffs,
                original=result,
            )
        )
    return normalized
