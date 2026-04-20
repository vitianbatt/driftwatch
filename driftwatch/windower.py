"""windower.py — sliding window analysis over drift results.

Groups DriftResult records into time-based windows and reports
how many services drifted within each window.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class WindowerError(Exception):
    """Raised when window configuration or input is invalid."""


@dataclass
class WindowConfig:
    size: int = 5          # number of results per window
    step: int = 1          # slide step between consecutive windows

    def __post_init__(self) -> None:
        if self.size < 1:
            raise WindowerError("size must be >= 1")
        if self.step < 1:
            raise WindowerError("step must be >= 1")
        if self.step > self.size:
            raise WindowerError("step must not exceed size")


@dataclass
class Window:
    index: int
    results: List[DriftResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def drift_count(self) -> int:
        return sum(1 for r in self.results if r.diffs)

    def drift_rate(self) -> float:
        if not self.results:
            return 0.0
        return self.drift_count() / len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "total": len(self),
            "drift_count": self.drift_count(),
            "drift_rate": round(self.drift_rate(), 4),
            "services": self.service_names(),
        }


@dataclass
class WindowedReport:
    windows: List[Window] = field(default_factory=list)
    config: Optional[WindowConfig] = None

    def total_windows(self) -> int:
        return len(self.windows)

    def peak_drift_window(self) -> Optional[Window]:
        if not self.windows:
            return None
        return max(self.windows, key=lambda w: w.drift_rate())


def build_windows(
    results: List[DriftResult],
    config: Optional[WindowConfig] = None,
) -> WindowedReport:
    """Slide a window over *results* and return a WindowedReport."""
    if results is None:
        raise WindowerError("results must not be None")
    cfg = config or WindowConfig()
    windows: List[Window] = []
    idx = 0
    start = 0
    while start < len(results):
        end = start + cfg.size
        windows.append(Window(index=idx, results=results[start:end]))
        idx += 1
        start += cfg.step
    return WindowedReport(windows=windows, config=cfg)
