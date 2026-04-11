"""formatter.py — transforms DriftResult lists into structured display-ready records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class FormatterError(Exception):
    """Raised when formatting fails."""


@dataclass
class FormattedRecord:
    service: str
    has_drift: bool
    drift_count: int
    field_summaries: List[str] = field(default_factory=list)
    raw_result: Optional[DriftResult] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift,
            "drift_count": self.drift_count,
            "field_summaries": self.field_summaries,
        }

    def one_line(self) -> str:
        if not self.has_drift:
            return f"{self.service}: OK"
        fields = ", ".join(self.field_summaries[:3])
        suffix = f" (+{self.drift_count - 3} more)" if self.drift_count > 3 else ""
        return f"{self.service}: DRIFT [{fields}{suffix}]"


def _summarise_diff(d: FieldDiff) -> str:
    if d.expected is None:
        return f"+{d.field}"
    if d.actual is None:
        return f"-{d.field}"
    return f"~{d.field}"


def format_results(results: List[DriftResult]) -> List[FormattedRecord]:
    """Convert a list of DriftResult objects into FormattedRecord instances."""
    if results is None:
        raise FormatterError("results must not be None")

    records: List[FormattedRecord] = []
    for result in results:
        diffs: List[FieldDiff] = getattr(result, "diffs", []) or []
        summaries = [_summarise_diff(d) for d in diffs]
        records.append(
            FormattedRecord(
                service=result.service,
                has_drift=result.has_drift,
                drift_count=len(diffs),
                field_summaries=summaries,
                raw_result=result,
            )
        )
    return records
