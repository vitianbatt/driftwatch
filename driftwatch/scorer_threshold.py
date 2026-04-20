"""scorer_threshold.py – filter ScoredResults by a minimum score threshold."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from driftwatch.scorer import ScoredResult, ScoredReport


class ScorerThresholdError(Exception):
    """Raised when scorer threshold configuration or input is invalid."""


@dataclass
class ThresholdConfig:
    min_score: float = 0.0
    include_clean: bool = False

    def __post_init__(self) -> None:
        if self.min_score < 0:
            raise ScorerThresholdError("min_score must be >= 0")


@dataclass
class ThresholdedReport:
    config: ThresholdConfig
    kept: List[ScoredResult] = field(default_factory=list)
    dropped: List[ScoredResult] = field(default_factory=list)

    @property
    def total_kept(self) -> int:
        return len(self.kept)

    @property
    def total_dropped(self) -> int:
        return len(self.dropped)

    def summary(self) -> str:
        return (
            f"threshold={self.config.min_score} "
            f"kept={self.total_kept} dropped={self.total_dropped}"
        )


def apply_threshold(
    report: ScoredReport, config: ThresholdConfig
) -> ThresholdedReport:
    """Return a ThresholdedReport keeping only results that meet the score."""
    if report is None:
        raise ScorerThresholdError("report must not be None")
    if config is None:
        raise ScorerThresholdError("config must not be None")

    kept: List[ScoredResult] = []
    dropped: List[ScoredResult] = []

    for result in report.results:
        if not result.has_drift and not config.include_clean:
            dropped.append(result)
            continue
        if result.score >= config.min_score:
            kept.append(result)
        else:
            dropped.append(result)

    return ThresholdedReport(config=config, kept=kept, dropped=dropped)
