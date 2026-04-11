"""Tests for driftwatch.correlator."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.correlator import (
    CorrelationGroup,
    CorrelationReport,
    CorrelatorError,
    correlate,
)


def _make(service: str, fields=None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


# ---------------------------------------------------------------------------
# CorrelationGroup
# ---------------------------------------------------------------------------

class TestCorrelationGroup:
    def test_size_reflects_services(self):
        g = CorrelationGroup(fields=["env"], services=["svc-a", "svc-b"])
        assert g.size() == 2

    def test_to_dict_sorts_fields_and_services(self):
        g = CorrelationGroup(fields=["z", "a"], services=["beta", "alpha"])
        d = g.to_dict()
        assert d["fields"] == ["a", "z"]
        assert d["services"] == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# CorrelationReport
# ---------------------------------------------------------------------------

class TestCorrelationReport:
    def test_total_groups_empty(self):
        assert CorrelationReport().total_groups() == 0

    def test_services_in_any_group_deduplicates(self):
        g1 = CorrelationGroup(fields=["env"], services=["svc-a", "svc-b"])
        g2 = CorrelationGroup(fields=["replicas"], services=["svc-b", "svc-c"])
        report = CorrelationReport(groups=[g1, g2])
        assert report.services_in_any_group() == ["svc-a", "svc-b", "svc-c"]

    def test_summary_no_groups(self):
        assert CorrelationReport().summary() == "No correlated drift groups found."

    def test_summary_with_groups(self):
        g = CorrelationGroup(fields=["env"], services=["svc-a", "svc-b"])
        report = CorrelationReport(groups=[g])
        text = report.summary()
        assert "1 correlation group" in text
        assert "env" in text
        assert "svc-a" in text


# ---------------------------------------------------------------------------
# correlate()
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(CorrelatorError):
        correlate(None)


def test_empty_list_returns_empty_report():
    report = correlate([])
    assert report.total_groups() == 0


def test_no_drift_produces_no_groups():
    results = [_make("svc-a"), _make("svc-b")]
    report = correlate(results)
    assert report.total_groups() == 0


def test_single_service_with_drift_not_grouped():
    results = [_make("svc-a", ["env"]), _make("svc-b")]
    report = correlate(results)
    assert report.total_groups() == 0


def test_two_services_same_fields_form_group():
    results = [_make("svc-a", ["env", "replicas"]), _make("svc-b", ["env", "replicas"])]
    report = correlate(results)
    assert report.total_groups() == 1
    assert sorted(report.groups[0].services) == ["svc-a", "svc-b"]


def test_different_field_sets_produce_separate_groups():
    results = [
        _make("svc-a", ["env"]),
        _make("svc-b", ["env"]),
        _make("svc-c", ["replicas"]),
        _make("svc-d", ["replicas"]),
    ]
    report = correlate(results)
    assert report.total_groups() == 2


def test_three_services_same_fields_all_in_one_group():
    results = [
        _make("svc-a", ["timeout"]),
        _make("svc-b", ["timeout"]),
        _make("svc-c", ["timeout"]),
    ]
    report = correlate(results)
    assert report.total_groups() == 1
    assert report.groups[0].size() == 3


def test_report_is_deterministic():
    results = [
        _make("svc-b", ["env"]),
        _make("svc-a", ["env"]),
    ]
    r1 = correlate(results)
    r2 = correlate(list(reversed(results)))
    assert r1.groups[0].to_dict() == r2.groups[0].to_dict()
