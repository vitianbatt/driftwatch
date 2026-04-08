"""Periodic drift-check scheduler for driftwatch."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class SchedulerError(Exception):
    """Raised when the scheduler encounters a fatal error."""


@dataclass
class ScheduleConfig:
    """Configuration for a periodic drift-check run."""

    interval_seconds: int
    max_runs: Optional[int] = None  # None means run indefinitely
    on_drift: Optional[Callable[[List], None]] = None
    on_error: Optional[Callable[[Exception], None]] = None
    tags: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise SchedulerError(
                f"interval_seconds must be positive, got {self.interval_seconds}"
            )
        if self.max_runs is not None and self.max_runs <= 0:
            raise SchedulerError(
                f"max_runs must be positive or None, got {self.max_runs}"
            )


def run_scheduled(
    check_fn: Callable[[], List],
    config: ScheduleConfig,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    """Run *check_fn* repeatedly according to *config*.

    Parameters
    ----------
    check_fn:
        Zero-argument callable that returns a list of DriftResult objects.
    config:
        Scheduling parameters.
    sleep_fn:
        Injected sleep implementation (override in tests).

    Returns
    -------
    int
        Total number of completed runs.
    """
    runs = 0
    while config.max_runs is None or runs < config.max_runs:
        try:
            results = check_fn()
            runs += 1
            logger.info("Scheduled run %d completed (%d results).", runs, len(results))
            drifted = [r for r in results if getattr(r, "has_drift", False)]
            if drifted and config.on_drift is not None:
                config.on_drift(drifted)
        except Exception as exc:  # noqa: BLE001
            logger.error("Scheduled run error: %s", exc)
            if config.on_error is not None:
                config.on_error(exc)

        if config.max_runs is None or runs < config.max_runs:
            sleep_fn(config.interval_seconds)

    return runs
