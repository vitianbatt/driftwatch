"""censor.py — strips or replaces sensitive field values in DriftResults."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class CensorError(Exception):
    """Raised when censoring configuration is invalid."""


_DEFAULT_PLACEHOLDER = "<censored>"


@dataclass
class CensorRule:
    """Matches a field name and replaces its value with a placeholder."""

    field_name: str
    placeholder: str = _DEFAULT_PLACEHOLDER

    def __post_init__(self) -> None:
        if not self.field_name or not self.field_name.strip():
            raise CensorError("field_name must be a non-empty string")
        if not self.placeholder:
            raise CensorError("placeholder must be a non-empty string")

    def matches(self, diff: FieldDiff) -> bool:
        return diff.field == self.field_name


@dataclass
class CensoredResult:
    """A DriftResult whose sensitive field values have been replaced."""

    service: str
    diffs: List[FieldDiff] = field(default_factory=list)
    censored_fields: List[str] = field(default_factory=list)

    def has_drift(self) -> bool:
        return bool(self.diffs)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "censored_fields": sorted(self.censored_fields),
            "diffs": [
                {"field": d.field, "kind": d.kind, "expected": d.expected, "actual": d.actual}
                for d in self.diffs
            ],
        }


def censor_results(
    results: List[DriftResult],
    rules: List[CensorRule],
    *,
    placeholder: Optional[str] = None,
) -> List[CensoredResult]:
    """Apply *rules* to each result, masking matched field values.

    Args:
        results: List of DriftResult objects to process.
        rules: Censor rules to apply.
        placeholder: Optional global placeholder override.

    Returns:
        List of CensoredResult objects.
    """
    if results is None:
        raise CensorError("results must not be None")
    if rules is None:
        raise CensorError("rules must not be None")

    censored: List[CensoredResult] = []
    for result in results:
        new_diffs: List[FieldDiff] = []
        censored_fields: List[str] = []
        for diff in result.diffs:
            matched_rule = next((r for r in rules if r.matches(diff)), None)
            if matched_rule:
                mask = placeholder if placeholder is not None else matched_rule.placeholder
                new_diffs.append(
                    FieldDiff(
                        field=diff.field,
                        kind=diff.kind,
                        expected=mask,
                        actual=mask,
                    )
                )
                censored_fields.append(diff.field)
            else:
                new_diffs.append(diff)
        censored.append(
            CensoredResult(
                service=result.service,
                diffs=new_diffs,
                censored_fields=censored_fields,
            )
        )
    return censored
