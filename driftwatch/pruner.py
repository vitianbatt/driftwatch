"""Pruner: removes stale or irrelevant DriftResults from a report based on age or field criteria."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from driftwatch.comparator import DriftResult


class PrunerError(Exception):
    """Raised when pruning configuration or input is invalid."""


@dataclass
class PruneConfig:
    max_age_seconds: Optional[int] = None  # prune results older than this
    excluded_fields: List[str] = field(default_factory=list)  # strip these drift fields
    drop_clean: bool = False  # if True, remove results with no drift

    def __post_init__(self) -> None:
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise PrunerError("max_age_seconds must be a positive integer")
        if not isinstance(self.excluded_fields, list):
            raise PrunerError("excluded_fields must be a list")


@dataclass
class PrunedReport:
    kept: List[DriftResult]
    removed_count: int

    def total(self) -> int:
        return len(self.kept) + self.removed_count

    def summary(self) -> str:
        if not self.kept and self.removed_count == 0:
            return "No results to prune."
        return (
            f"Kept {len(self.kept)} result(s), pruned {self.removed_count} "
            f"from {self.total()} total."
        )


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def prune(
    results: List[DriftResult],
    config: PruneConfig,
    now: Optional[datetime] = None,
) -> PrunedReport:
    """Apply pruning rules and return a PrunedReport."""
    if results is None:
        raise PrunerError("results must not be None")
    if config is None:
        raise PrunerError("config must not be None")

    now = now or _utcnow()
    kept: List[DriftResult] = []
    removed = 0

    for result in results:
        # Drop clean results if requested
        if config.drop_clean and not result.diffs:
            removed += 1
            continue

        # Age-based pruning: result must carry a timestamp attribute
        if config.max_age_seconds is not None:
            ts = getattr(result, "timestamp", None)
            if ts is not None:
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts)
                    except ValueError:
                        ts = None
                if ts is not None:
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    age = (now - ts).total_seconds()
                    if age > config.max_age_seconds:
                        removed += 1
                        continue

        # Strip excluded drift fields
        if config.excluded_fields and result.diffs:
            filtered = [
                d for d in result.diffs
                if getattr(d, "field", None) not in config.excluded_fields
            ]
            result = DriftResult(
                service=result.service,
                diffs=filtered,
            )

        kept.append(result)

    return PrunedReport(kept=kept, removed_count=removed)
