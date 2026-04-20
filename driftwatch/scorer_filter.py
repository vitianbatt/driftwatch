"""scorer_filter.py — filter ScoredResults by score thresholds."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.scorer import ScoredResult, ScoredReport


class ScorerFilterError(Exception):
    """Raised when scorer filter configuration or input is invalid."""


@dataclass
class ScoreFilterConfig:
    min_score: float = 0.0
    max_score: Optional[float] = None
    include_clean: bool = True

    def __post_init__(self) -> None:
        if self.min_score < 0:
            raise ScorerFilterError("min_score must be >= 0")
        if self.max_score is not None and self.max_score < self.min_score:
            raise ScorerFilterError("max_score must be >= min_score")


@dataclass
class FilteredScoredReport:
    results: List[ScoredResult] = field(default_factory=list)
    total_input: int = 0
    total_excluded: int = 0

    @property
    def total_kept(self) -> int:
        return len(self.results)

    def summary(self) -> str:
        return (
            f"kept={self.total_kept} excluded={self.total_excluded} "
            f"of total={self.total_input}"
        )


def filter_scored(report: ScoredReport, config: ScoreFilterConfig) -> FilteredScoredReport:
    """Return a FilteredScoredReport containing only results that pass the config."""
    if report is None:
        raise ScorerFilterError("report must not be None")
    if config is None:
        raise ScorerFilterError("config must not be None")

    kept: List[ScoredResult] = []
    excluded = 0

    for r in report.results:
        if not config.include_clean and r.score == 0.0:
            excluded += 1
            continue
        if r.score < config.min_score:
            excluded += 1
            continue
        if config.max_score is not None and r.score > config.max_score:
            excluded += 1
            continue
        kept.append(r)

    return FilteredScoredReport(
        results=kept,
        total_input=len(report.results),
        total_excluded=excluded,
    )
