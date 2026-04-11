"""masker.py — redacts or masks sensitive field values in DriftResults before reporting."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import re

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class MaskerError(Exception):
    """Raised when masking configuration or input is invalid."""


_DEFAULT_MASK = "***"


@dataclass
class MaskRule:
    """A rule that masks field values whose names match a pattern."""

    pattern: str
    mask: str = _DEFAULT_MASK

    def __post_init__(self) -> None:
        if not self.pattern or not self.pattern.strip():
            raise MaskerError("MaskRule pattern must not be empty")
        if not self.mask:
            raise MaskerError("MaskRule mask must not be empty")
        try:
            re.compile(self.pattern)
        except re.error as exc:
            raise MaskerError(f"Invalid regex pattern {self.pattern!r}: {exc}") from exc

    def matches(self, field_name: str) -> bool:
        return bool(re.search(self.pattern, field_name))


@dataclass
class MaskedResult:
    """A DriftResult whose sensitive diff values have been masked."""

    service: str
    has_drift: bool
    diffs: List[FieldDiff] = field(default_factory=list)
    masked_fields: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift,
            "diffs": [
                {"field": d.field, "kind": d.kind, "expected": d.expected, "actual": d.actual}
                for d in self.diffs
            ],
            "masked_fields": self.masked_fields,
        }


def _mask_diff(diff: FieldDiff, mask: str) -> FieldDiff:
    """Return a new FieldDiff with expected/actual replaced by the mask string."""
    return FieldDiff(
        field=diff.field,
        kind=diff.kind,
        expected=mask,
        actual=mask,
    )


def mask_results(
    results: List[DriftResult],
    rules: List[MaskRule],
) -> List[MaskedResult]:
    """Apply mask rules to a list of DriftResults, returning MaskedResults."""
    if results is None:
        raise MaskerError("results must not be None")
    if rules is None:
        raise MaskerError("rules must not be None")

    masked: List[MaskedResult] = []
    for result in results:
        new_diffs: List[FieldDiff] = []
        masked_fields: List[str] = []
        for diff in result.diffs:
            applied: Optional[str] = None
            for rule in rules:
                if rule.matches(diff.field):
                    applied = rule.mask
                    break
            if applied is not None:
                new_diffs.append(_mask_diff(diff, applied))
                masked_fields.append(diff.field)
            else:
                new_diffs.append(diff)
        masked.append(
            MaskedResult(
                service=result.service,
                has_drift=result.has_drift,
                diffs=new_diffs,
                masked_fields=masked_fields,
            )
        )
    return masked
