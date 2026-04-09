"""Throttler: rate-limits drift notifications to avoid alert fatigue."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


class ThrottlerError(Exception):
    """Raised when throttler configuration is invalid."""


@dataclass
class ThrottleRule:
    service: str
    min_interval_seconds: int

    def __post_init__(self) -> None:
        if not self.service or not self.service.strip():
            raise ThrottlerError("service must be a non-empty string")
        if self.min_interval_seconds <= 0:
            raise ThrottlerError("min_interval_seconds must be positive")


@dataclass
class ThrottledReport:
    allowed: list  # results that passed throttle
    suppressed: list  # results that were throttled

    def total_allowed(self) -> int:
        return len(self.allowed)

    def total_suppressed(self) -> int:
        return len(self.suppressed)

    def summary(self) -> str:
        if not self.allowed and not self.suppressed:
            return "No results to throttle."
        parts = []
        if self.allowed:
            parts.append(f"{self.total_allowed()} allowed")
        if self.suppressed:
            parts.append(f"{self.total_suppressed()} suppressed")
        return "; ".join(parts) + "."


class Throttler:
    """Tracks last-notification times and enforces per-service rate limits."""

    def __init__(self, rules: list[ThrottleRule], default_interval_seconds: int = 300) -> None:
        if default_interval_seconds <= 0:
            raise ThrottlerError("default_interval_seconds must be positive")
        self._rules: Dict[str, ThrottleRule] = {r.service: r for r in (rules or [])}
        self._default_interval = default_interval_seconds
        self._last_seen: Dict[str, datetime] = {}

    def _interval_for(self, service: str) -> int:
        rule = self._rules.get(service)
        return rule.min_interval_seconds if rule else self._default_interval

    def is_allowed(self, service: str, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        last = self._last_seen.get(service)
        if last is None:
            return True
        interval = self._interval_for(service)
        return (now - last) >= timedelta(seconds=interval)

    def record(self, service: str, now: Optional[datetime] = None) -> None:
        self._last_seen[service] = now or datetime.utcnow()


def apply_throttle(results: list, throttler: Throttler, now: Optional[datetime] = None) -> ThrottledReport:
    """Filter results through the throttler; record timestamps for allowed ones."""
    if results is None:
        raise ThrottlerError("results must not be None")
    if throttler is None:
        raise ThrottlerError("throttler must not be None")
    allowed, suppressed = [], []
    ts = now or datetime.utcnow()
    for result in results:
        service = getattr(result, "service", None)
        if service is None:
            raise ThrottlerError(f"result missing 'service' attribute: {result!r}")
        if throttler.is_allowed(service, ts):
            throttler.record(service, ts)
            allowed.append(result)
        else:
            suppressed.append(result)
    return ThrottledReport(allowed=allowed, suppressed=suppressed)
