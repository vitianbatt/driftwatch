"""Tests for driftwatch.comparator module."""

import pytest

from driftwatch.comparator import DriftCompareError, DriftResult, compare


SAMPLE_SPEC = {
    "service": "auth-service",
    "version": "1.2.0",
    "replicas": 3,
    "environment": "production",
    "port": 8080,
}

SAMPLE_LIVE_MATCHING = {
    "service": "auth-service",
    "version": "1.2.0",
    "replicas": 3,
    "environment": "production",
    "port": 8080,
}

SAMPLE_LIVE_DRIFTED = {
    "service": "auth-service",
    "version": "1.3.0",
    "replicas": 5,
    "environment": "staging",
    "port": 9090,
    "unknown_field": "injected-at-runtime",
}


class TestDriftResult:
    def test_no_drift_when_empty(self):
        result = DriftResult(service="svc")
        assert not result.has_drift

    def test_has_drift_with_missing_keys(self):
        result = DriftResult(service="svc", missing_keys=["port"])
        assert result.has_drift

    def test_summary_no_drift(self):
        result = DriftResult(service="svc")
        assert "No drift detected" in result.summary()

    def test_summary_with_drift(self):
        result = DriftResult(
            service="svc",
            missing_keys=["port"],
            extra_keys=["debug"],
            mismatched_values={"version": {"expected": "1.0", "actual": "2.0"}},
        )
        summary = result.summary()
        assert "missing key: port" in summary
        assert "extra key:   debug" in summary
        assert "version" in summary
        assert "1.0" in summary
        assert "2.0" in summary


class TestCompare:
    def test_identical_configs_no_drift(self):
        result = compare(SAMPLE_SPEC, SAMPLE_LIVE_MATCHING, service="auth-service")
        assert not result.has_drift
        assert result.service == "auth-service"

    def test_detects_mismatched_values(self):
        result = compare(SAMPLE_SPEC, SAMPLE_LIVE_DRIFTED, service="auth-service")
        assert "version" in result.mismatched_values
        assert result.mismatched_values["version"]["expected"] == "1.2.0"
        assert result.mismatched_values["version"]["actual"] == "1.3.0"

    def test_detects_extra_keys(self):
        result = compare(SAMPLE_SPEC, SAMPLE_LIVE_DRIFTED, service="auth-service")
        assert "unknown_field" in result.extra_keys

    def test_detects_missing_keys(self):
        live = {k: v for k, v in SAMPLE_LIVE_MATCHING.items() if k != "port"}
        result = compare(SAMPLE_SPEC, live, service="auth-service")
        assert "port" in result.missing_keys

    def test_raises_on_non_dict_spec(self):
        with pytest.raises(DriftCompareError):
            compare(["not", "a", "dict"], SAMPLE_LIVE_MATCHING)

    def test_raises_on_non_dict_live(self):
        with pytest.raises(DriftCompareError):
            compare(SAMPLE_SPEC, "not-a-dict")

    def test_default_service_name(self):
        result = compare(SAMPLE_SPEC, SAMPLE_LIVE_MATCHING)
        assert result.service == "unknown"
