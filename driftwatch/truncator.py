"""Truncator: limits the number of drift fields reported per service result."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class TruncatorError(Exception):
    """Raised when truncation configuration is invalid."""


@dataclass
class TruncateConfig:
    max_diffs: int = 10

    def __post_init__(self) -> None:
        if self.max_diffs < 1:
            raise TruncatorError("max_diffs must be at least 1")


@dataclass
class TruncatedResult:
    service: str
    diffs: List[FieldDiff]
    truncated_count: int = 0

    @property
    def has_drift(self) -> bool:
        return len(self.diffs) > 0 or self.truncated_count > 0

    @property
    def was_truncated(self) -> bool:
        return self.truncated_count > 0

    def summary(self) -> str:
        parts = [f"{self.service}: {len(self.diffs)} diff(s) shown"]
        if self.was_truncated:
            parts.append(f"{self.truncated_count} suppressed")
        return ", ".join(parts)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "diffs": [str(d) for d in self.diffs],
            "truncated_count": self.truncated_count,
            "was_truncated": self.was_truncated,
        }


@dataclass
class TruncateReport:
    results: List[TruncatedResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def any_truncated(self) -> bool:
        return any(r.was_truncated for r in self.results)


def truncate_results(
    results: List[DriftResult],
    config: Optional[TruncateConfig] = None,
) -> TruncateReport:
    """Apply per-service diff truncation to a list of DriftResults."""
    if results is None:
        raise TruncatorError("results must not be None")
    if config is None:
        config = TruncateConfig()

    truncated: List[TruncatedResult] = []
    for r in results:
        diffs = list(r.diffs) if r.diffs else []
        kept = diffs[: config.max_diffs]
        dropped = len(diffs) - len(kept)
        truncated.append(
            TruncatedResult(
                service=r.service,
                diffs=kept,
                truncated_count=dropped,
            )
        )
    return TruncateReport(results=truncated)
