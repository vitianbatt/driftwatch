"""Tests for driftwatch.filter module."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.filter import FilterError, Severity, _result_severity, filter_results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(service: str, missing=(), extra=(), changed=None) -> DriftResult:
    return DriftResult(
        service_name=service,
        missing_keys=list(missing),
        extra_keys=list(extra),
        changed_values=changed or {},
    )


HIGH_DRIFT = _make("auth-service", missing=["a", "b", "c", "d", "e", "f"])
MED_DRIFT = _make("billing-service", missing=["x"], extra=["y"], changed={"z": {}})
LOW_DRIFT = _make("auth-proxy", missing=["ssl_cert"])
NO_DRIFT = _make("notification-service")

ALL_RESULTS = [HIGH_DRIFT, MED_DRIFT, LOW_DRIFT, NO_DRIFT]


# ---------------------------------------------------------------------------
# _result_severity
# ---------------------------------------------------------------------------

class TestResultSeverity:
    def test_no_drift_is_low(self):
        assert _result_severity(NO_DRIFT) == Severity.LOW

    def test_one_field_is_low(self):
        assert _result_severity(LOW_DRIFT) == Severity.LOW

    def test_three_fields_is_medium(self):
        assert _result_severity(MED_DRIFT) == Severity.MEDIUM

    def test_six_fields_is_high(self):
        assert _result_severity(HIGH_DRIFT) == Severity.HIGH


# ---------------------------------------------------------------------------
# filter_results — only_drift
# ---------------------------------------------------------------------------

class TestFilterOnlyDrift:
    def test_only_drift_excludes_clean(self):
        out = filter_results(ALL_RESULTS, only_drift=True)
        assert NO_DRIFT not in out

    def test_only_drift_keeps_drifted(self):
        out = filter_results(ALL_RESULTS, only_drift=True)
        assert HIGH_DRIFT in out and MED_DRIFT in out and LOW_DRIFT in out

    def test_false_keeps_all(self):
        out = filter_results(ALL_RESULTS, only_drift=False)
        assert len(out) == len(ALL_RESULTS)


# ---------------------------------------------------------------------------
# filter_results — service name
# ---------------------------------------------------------------------------

class TestFilterByService:
    def test_substring_match(self):
        out = filter_results(ALL_RESULTS, service="auth")
        names = {r.service_name for r in out}
        assert names == {"auth-service", "auth-proxy"}

    def test_case_insensitive(self):
        out = filter_results(ALL_RESULTS, service="AUTH")
        assert len(out) == 2

    def test_no_match_returns_empty(self):
        out = filter_results(ALL_RESULTS, service="unknown")
        assert out == []


# ---------------------------------------------------------------------------
# filter_results — min_severity
# ---------------------------------------------------------------------------

class TestFilterBySeverity:
    def test_min_high_returns_only_high(self):
        out = filter_results(ALL_RESULTS, min_severity=Severity.HIGH)
        assert out == [HIGH_DRIFT]

    def test_min_medium_excludes_low_and_none(self):
        out = filter_results(ALL_RESULTS, min_severity=Severity.MEDIUM)
        assert HIGH_DRIFT in out and MED_DRIFT in out
        assert LOW_DRIFT not in out and NO_DRIFT not in out

    def test_min_low_returns_all(self):
        out = filter_results(ALL_RESULTS, min_severity=Severity.LOW)
        assert len(out) == len(ALL_RESULTS)


# ---------------------------------------------------------------------------
# filter_results — invalid input
# ---------------------------------------------------------------------------

def test_non_list_raises_filter_error():
    with pytest.raises(FilterError):
        filter_results("not-a-list")  # type: ignore
