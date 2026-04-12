"""Tests for driftwatch/stamper.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.stamper import (
    StamperError,
    StampedResult,
    StampReport,
    stamp_results,
)


def _make(service: str, fields=None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


# ---------------------------------------------------------------------------
# StampedResult
# ---------------------------------------------------------------------------

class TestStampedResult:
    def test_has_drift_false_when_empty(self):
        r = StampedResult(service="svc", drifted_fields=[], stamp="v1")
        assert r.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        r = StampedResult(service="svc", drifted_fields=["env"], stamp="v1")
        assert r.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        r = StampedResult(service="svc", drifted_fields=["env"], stamp="abc", source="ci")
        d = r.to_dict()
        assert set(d.keys()) == {"service", "drifted_fields", "stamp", "source"}

    def test_to_dict_values(self):
        r = StampedResult(service="auth", drifted_fields=["replicas"], stamp="sha-123", source="deploy")
        d = r.to_dict()
        assert d["service"] == "auth"
        assert d["stamp"] == "sha-123"
        assert d["source"] == "deploy"
        assert d["drifted_fields"] == ["replicas"]

    def test_source_defaults_to_none(self):
        r = StampedResult(service="svc", drifted_fields=[], stamp="v2")
        assert r.source is None


# ---------------------------------------------------------------------------
# StampReport
# ---------------------------------------------------------------------------

class TestStampReport:
    def test_len_reflects_results(self):
        report = StampReport(results=[StampedResult("a", [], "v1"), StampedResult("b", [], "v1")])
        assert len(report) == 2

    def test_service_names(self):
        report = StampReport(results=[StampedResult("alpha", [], "v1"), StampedResult("beta", [], "v1")])
        assert report.service_names() == ["alpha", "beta"]

    def test_summary_no_drift(self):
        report = StampReport(results=[StampedResult("svc", [], "v1")])
        assert "0/1" in report.summary()

    def test_summary_with_drift(self):
        report = StampReport(
            results=[
                StampedResult("a", ["env"], "v1"),
                StampedResult("b", [], "v1"),
            ]
        )
        assert "1/2" in report.summary()


# ---------------------------------------------------------------------------
# stamp_results
# ---------------------------------------------------------------------------

def test_none_results_raises():
    with pytest.raises(StamperError, match="None"):
        stamp_results(None, "v1")


def test_empty_stamp_raises():
    with pytest.raises(StamperError, match="non-empty"):
        stamp_results([], "")


def test_whitespace_stamp_raises():
    with pytest.raises(StamperError, match="non-empty"):
        stamp_results([], "   ")


def test_empty_results_returns_empty_report():
    report = stamp_results([], "v1")
    assert len(report) == 0


def test_stamp_applied_to_all():
    results = [_make("svc-a"), _make("svc-b", ["field"])]
    report = stamp_results(results, "sha-999")
    assert all(r.stamp == "sha-999" for r in report.results)


def test_source_propagated():
    results = [_make("svc")]
    report = stamp_results(results, "v2", source="pipeline")
    assert report.results[0].source == "pipeline"


def test_drifted_fields_preserved():
    results = [_make("svc", ["replicas", "image"])]
    report = stamp_results(results, "v3")
    assert report.results[0].drifted_fields == ["replicas", "image"]


def test_service_name_preserved():
    results = [_make("my-service")]
    report = stamp_results(results, "v1")
    assert report.results[0].service == "my-service"
