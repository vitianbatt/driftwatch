"""clamper.py — clamp drift field counts to a configured maximum per service."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class ClamperError(Exception):
    """Raised when clamping configuration is invalid."""


@dataclass
class ClampConfig:
    max_diffs: int = 5
    truncation_marker: str = "..."

    def __post_init__(self) -> None:
        if self.max_diffs < 1:
            raise ClamperError("max_diffs must be at least 1")
        if not self.truncation_marker:
            raise ClamperError("truncation_marker must not be empty")


@dataclass
class ClampedResult:
    service: str
    drifted_fields: List[FieldDiff]
    truncated: bool
    original_count: int

    def has_drift(self) -> bool:
        return len(self.drifted_fields) > 0

    def summary(self) -> str:
        if not self.has_drift():
            return f"{self.service}: clean"
        note = f" (+{self.original_count - len(self.drifted_fields)} truncated)" if self.truncated else ""
        return f"{self.service}: {len(self.drifted_fields)} drifted field(s){note}"

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "drifted_fields": [str(d) for d in self.drifted_fields],
            "truncated": self.truncated,
            "original_count": self.original_count,
        }


def clamp_results(
    results: List[DriftResult],
    config: Optional[ClampConfig] = None,
) -> List[ClampedResult]:
    """Return a list of ClampedResult, each with at most config.max_diffs drifted fields."""
    if results is None:
        raise ClamperError("results must not be None")
    if config is None:
        config = ClampConfig()

    clamped: List[ClampedResult] = []
    for result in results:
        diffs = list(result.drifted_fields)
        original_count = len(diffs)
        truncated = original_count > config.max_diffs
        visible = diffs[: config.max_diffs]
        clamped.append(
            ClampedResult(
                service=result.service,
                drifted_fields=visible,
                truncated=truncated,
                original_count=original_count,
            )
        )
    return clamped
