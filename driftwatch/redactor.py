"""Redactor: mask sensitive field values in drift results before output."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff

_MASK = "***REDACTED***"


class RedactorError(Exception):
    """Raised when redaction configuration is invalid."""


@dataclass
class RedactRule:
    """A rule that redacts fields whose names match a pattern."""

    pattern: str
    mask: str = _MASK

    def __post_init__(self) -> None:
        if not self.pattern or not self.pattern.strip():
            raise RedactorError("pattern must be a non-empty string")
        try:
            re.compile(self.pattern)
        except re.error as exc:
            raise RedactorError(f"invalid regex pattern {self.pattern!r}: {exc}") from exc

    def matches(self, field_name: str) -> bool:
        return bool(re.search(self.pattern, field_name))


@dataclass
class RedactedResult:
    """A drift result with sensitive field values masked."""

    service: str
    diffs: List[FieldDiff]
    redacted_fields: List[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.diffs)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift,
            "redacted_fields": self.redacted_fields,
            "diffs": [
                {"field": d.field, "kind": d.kind, "expected": d.expected, "actual": d.actual}
                for d in self.diffs
            ],
        }


def _redact_diff(diff: FieldDiff, mask: str) -> FieldDiff:
    return FieldDiff(
        field=diff.field,
        kind=diff.kind,
        expected=mask if diff.expected is not None else None,
        actual=mask if diff.actual is not None else None,
    )


def redact_results(
    results: List[DriftResult],
    rules: List[RedactRule],
) -> List[RedactedResult]:
    """Apply redaction rules to a list of DriftResults."""
    if results is None:
        raise RedactorError("results must not be None")
    if rules is None:
        raise RedactorError("rules must not be None")

    redacted: List[RedactedResult] = []
    for result in results:
        new_diffs: List[FieldDiff] = []
        redacted_fields: List[str] = []
        for diff in result.diffs:
            matched_rule: Optional[RedactRule] = next(
                (r for r in rules if r.matches(diff.field)), None
            )
            if matched_rule:
                new_diffs.append(_redact_diff(diff, matched_rule.mask))
                redacted_fields.append(diff.field)
            else:
                new_diffs.append(diff)
        redacted.append(
            RedactedResult(
                service=result.service,
                diffs=new_diffs,
                redacted_fields=redacted_fields,
            )
        )
    return redacted
