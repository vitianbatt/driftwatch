"""Tests for driftwatch.aggregator."""

import pytest
from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity
from driftwatch.aggregator import (
    AggregatorError,
    ServiceSummary,
    AggregateReport,
    aggregate,
)


def _make(service: str, missing=None, extra=None, changed=None) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=missing or [],
        extra_keys=extra or [],
        changed_keys=changed or {},
    )


class TestAggregate:
    def test_empty_list_returns_zero_counts(self):
        report = aggregate([])
        assert report.total_services == 0
        assert report.drifted_services == 0
        assert report.clean_services == 0
        assert report.drift_rate == 0.0

    def test_none_raises(self):
        with pytest.raises(AggregatorError):
            aggregate(None)

    def test_all_clean_services(self):
        results = [_make("svc-a"), _make("svc-b")]
        report = aggregate(results)
        assert report.total_services == 2
        assert report.drifted_services == 0
        assert report.clean_services == 2

    def test_mixed_drift(self):
        results = [
            _make("svc-a"),
            _make("svc-b", missing=["key1"]),
            _make("svc-c", changed={"key2": ("old", "new")}),
        ]
        report = aggregate(results)
        assert report.total_services == 3
        assert report.drifted_services == 2
        assert report.clean_services == 1

    def test_drift_rate_calculation(self):
        results = [_make("svc-a", missing=["k"]), _make("svc-b"), _make("svc-c"), _make("svc-d")]
        report = aggregate(results)
        assert report.drift_rate == 0.25

    def test_severity_counts_populated(self):
        results = [
            _make("svc-a"),
            _make("svc-b", missing=["k1", "k2", "k3"]),
        ]
        report = aggregate(results)
        assert isinstance(report.severity_counts, dict)
        assert sum(report.severity_counts.values()) == 2

    def test_summaries_length_matches_results(self):
        results = [_make(f"svc-{i}") for i in range(5)]
        report = aggregate(results)
        assert len(report.summaries) == 5

    def test_summary_service_name_preserved(self):
        results = [_make("auth-service", missing=["timeout"])]
        report = aggregate(results)
        assert report.summaries[0].service == "auth-service"
        assert report.summaries[0].has_drift is True

    def test_drift_field_count_correct(self):
        results = [_make("svc", missing=["a"], extra=["b"], changed={"c": (1, 2)})]
        report = aggregate(results)
        assert report.summaries[0].drift_field_count == 3

    def test_to_dict_contains_expected_keys(self):
        report = aggregate([_make("svc-a")])
        d = report.to_dict()
        assert "total_services" in d
        assert "drift_rate" in d
        assert "severity_counts" in d
        assert "summaries" in d

    def test_service_summary_to_dict(self):
        s = ServiceSummary(service="x", has_drift=True, drift_field_count=2, severity=Severity.MEDIUM)
        d = s.to_dict()
        assert d["service"] == "x"
        assert d["severity"] == "medium"
