"""Compare a live config against a saved baseline snapshot."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from driftwatch.baseline import BaselineEntry, BaselineError, load_baseline
from driftwatch.comparator import DriftResult, compare


class BaselineCompareError(Exception):
    """Raised when a baseline comparison cannot be completed."""


@dataclass
class BaselineDriftReport:
    service: str
    baseline_entry: Optional[BaselineEntry]
    drift_result: Optional[DriftResult]
    error: Optional[str] = None

    @property
    def has_baseline(self) -> bool:
        return self.baseline_entry is not None

    @property
    def has_drift(self) -> bool:
        return (
            self.drift_result is not None
            and bool(self.drift_result.missing_keys or self.drift_result.extra_keys or self.drift_result.changed_keys)
        )

    def summary(self) -> str:
        if self.error:
            return f"[{self.service}] error: {self.error}"
        if not self.has_baseline:
            return f"[{self.service}] no baseline recorded"
        if self.has_drift:
            dr = self.drift_result
            parts: List[str] = []
            if dr.missing_keys:
                parts.append(f"missing={dr.missing_keys}")
            if dr.extra_keys:
                parts.append(f"extra={dr.extra_keys}")
            if dr.changed_keys:
                parts.append(f"changed={dr.changed_keys}")
            return f"[{self.service}] drift detected: {', '.join(parts)}"
        return f"[{self.service}] matches baseline"


def compare_to_baseline(
    service: str,
    live_config: Dict[str, Any],
    baseline_path: str | Path,
) -> BaselineDriftReport:
    """Load the latest baseline for *service* and diff against *live_config*."""
    try:
        entry = load_baseline(baseline_path, service)
    except BaselineError as exc:
        return BaselineDriftReport(service=service, baseline_entry=None, drift_result=None, error=str(exc))

    if entry is None:
        return BaselineDriftReport(service=service, baseline_entry=None, drift_result=None)

    result = compare(spec=entry.snapshot, live=live_config)
    return BaselineDriftReport(service=service, baseline_entry=entry, drift_result=result)
