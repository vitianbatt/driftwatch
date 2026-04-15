"""leveler.py — assigns drift severity levels to results based on field count thresholds."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from driftwatch.comparator import DriftResult


class LevelerError(Exception):
    """Raised when leveling configuration or input is invalid."""


class DriftLevel(str, Enum):
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class LevelConfig:
    low_threshold: int = 1
    medium_threshold: int = 3
    high_threshold: int = 6

    def __post_init__(self) -> None:
        for name, val in [
            ("low_threshold", self.low_threshold),
            ("medium_threshold", self.medium_threshold),
            ("high_threshold", self.high_threshold),
        ]:
            if val < 1:
                raise LevelerError(f"{name} must be >= 1, got {val}")
        if not (self.low_threshold <= self.medium_threshold <= self.high_threshold):
            raise LevelerError(
                "thresholds must satisfy low <= medium <= high"
            )


@dataclass
class LeveledResult:
    service: str
    level: DriftLevel
    drift_field_count: int
    drifted_fields: List[str] = field(default_factory=list)

    def has_drift(self) -> bool:
        return self.level != DriftLevel.NONE

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "level": self.level.value,
            "drift_field_count": self.drift_field_count,
            "drifted_fields": list(self.drifted_fields),
        }


def _compute_level(count: int, cfg: LevelConfig) -> DriftLevel:
    if count == 0:
        return DriftLevel.NONE
    if count < cfg.low_threshold + 1:
        return DriftLevel.LOW
    if count < cfg.medium_threshold + 1:
        return DriftLevel.MEDIUM
    if count < cfg.high_threshold + 1:
        return DriftLevel.HIGH
    return DriftLevel.CRITICAL


def level_results(
    results: Optional[List[DriftResult]],
    config: Optional[LevelConfig] = None,
) -> List[LeveledResult]:
    """Assign a DriftLevel to each DriftResult."""
    if results is None:
        raise LevelerError("results must not be None")
    if config is None:
        config = LevelConfig()
    leveled: List[LeveledResult] = []
    for r in results:
        fields = list(r.drifted_fields) if r.drifted_fields else []
        count = len(fields)
        lvl = _compute_level(count, config)
        leveled.append(
            LeveledResult(
                service=r.service,
                level=lvl,
                drift_field_count=count,
                drifted_fields=fields,
            )
        )
    return leveled
