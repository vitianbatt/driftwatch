"""Tests for driftwatch.throttler."""
from __future__ import annotations

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from driftwatch.throttler import (
    ThrottlerError,
    ThrottleRule,
    ThrottledReport,
    Throttler,
    apply_throttle,
)


def _make(service: str, drift: bool = True):
    r = MagicMock()
    r.service = service
    r.has_drift = drift
    return r


class TestThrottleRule:
    def test_valid_rule_created(self):
        rule = ThrottleRule(service="auth", min_interval_seconds=60)
        assert rule.service == "auth"
        assert rule.min_interval_seconds == 60

    def test_empty_service_raises(self):
        with pytest.raises(ThrottlerError, match="non-empty"):
            ThrottleRule(service="", min_interval_seconds=60)

    def test_whitespace_service_raises(self):
        with pytest.raises(ThrottlerError, match="non-empty"):
            ThrottleRule(service="   ", min_interval_seconds=60)

    def test_zero_interval_raises(self):
        with pytest.raises(ThrottlerError, match="positive"):
            ThrottleRule(service="auth", min_interval_seconds=0)

    def test_negative_interval_raises(self):
        with pytest.raises(ThrottlerError, match="positive"):
            ThrottleRule(service="auth", min_interval_seconds=-10)


class TestThrottler:
    def test_first_call_always_allowed(self):
        t = Throttler(rules=[])
        assert t.is_allowed("auth") is True

    def test_second_call_within_interval_supp = datetime(2024, 1, 1, 12, 0, 0)
        t = Throttler(rules=[], default_interval_seconds=300)
        t.record("auth", now)
        soon = now + timedelta(seconds=60)
        assert t.is_allowed("auth", soon) is False

    def test_call_after_interval_allowed(self):
        now = datetime(2024, 1, 1, n        t = Throttler(rules=[], default_interval_seconds=300)
        t.record("auth", now)
        later = now + timedelta(seconds=300)
        assert t.is_allowed("auth", later) is True

    def test_per_service_rule_overrides_default(self):
        rule = ThrottleRule(service="auth", min_interval_seconds=30)
        now = datetime(2024, 1, 1, 12, 0, 0)
        t = Throttler(rules=[rule], default_interval_seconds=300)
        t.record("auth", now)
        after_rule = now + timedelta(seconds=31)
        assert t.is_allowed("auth", after_rule) is True

    def test_invalid_default_interval_raises(self):
        with pytest.raises(ThrottlerError, match="positive"):
            Throttler(rules=[], default_interval_seconds=0)


class TestApplyThrottle:
    def test_empty_results_returns_empty_report(self):
        t = Throttler(rules=[])
        report = apply_throttle([], t)
        assert report.total_allowed() == 0
        assert report.total_suppressed() == 0

    def test_none_results_raises(self):
        t = Throttler(rules=[])
        with pytest.raises(ThrottlerError):
            apply_throttle(None, t)

    def test_none_throttler_raises(self):
        with pytest.raises(ThrottlerError):
            apply_throttle([], None)

    def test_first_results_all_allowed(self):
        t = Throttler(rules=[])
        results = [_make("auth"), _make("billing")]
        report = apply_throttle(results, t)
        assert report.total_allowed() == 2
        assert report.total_suppressed() == 0

    def test_duplicate_within_interval_suppressed(self):
        now = datetime(2024, 1, 1, 12, 0, 0)
        t = Throttler(rules=[], default_interval_seconds=300)
        results = [_make("auth"), _make("auth")]
        report = apply_throttle(results, t, now=now)
        assert report.total_allowed() == 1
        assert report.total_suppressed() == 1

    def test_result_missing_service_raises(self):
        t = Throttler(rules=[])
        bad = MagicMock(spec=[])
        with pytest.raises(ThrottlerError, match="service"):
            apply_throttle([bad], t)


class TestThrottledReport:
    def test_summary_empty(self):
        r = ThrottledReport(allowed=[], suppressed=[])
        assert r.summary() == "No results to throttle."

    def test_summary_all_allowed(self):
        r = ThrottledReport(allowed=[_make("auth")], suppressed=[])
        assert "1 allowed" in r.summary()

    def test_summary_mixed(self):
        r = ThrottledReport(allowed=[_make("auth")], suppressed=[_make("billing")])
        assert "1 allowed" in r.summary()
        assert "1 suppressed" in r.summary()
