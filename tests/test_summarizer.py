"""Tests for driftwatch.summarizer."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.summarizer import (
    SummarizerError,
    SummaryReport,
    ServiceSummary,
    summarize,
)


def _make(service: str, spec: dict = None, diffs: dict = None) -> DriftResult:
    return DriftResult(
        service=service,
        spec=spec or {},
        live={},
        diffs=diffs or {},
    )


class TestServiceSummary:
    def test_has_drift_false_when_no_drifted_fields(self):
        s = ServiceSummary(service="auth", total_fields=3, drifted_fields=[])
        assert s.has_drift is False

    def test_has_drift_true_when_drifted_fields_present(self):
        s = ServiceSummary(service="auth", total_fields=3, drifted_fields=["replicas"])
        assert s.has_drift is True

    def test_to_dict_contains_all_keys(self):
        s = ServiceSummary(service="auth", total_fields=2, drifted_fields=["image"])
        d = s.to_dict()
        assert set(d.keys()) == {"service", "total_fields", "drifted_fields", "has_drift"}

    def test_to_dict_values(self):
        s = ServiceSummary(service="gateway", total_fields=5, drifted_fields=["env", "replicas"])
        d = s.to_dict()
        assert d["service"] == "gateway"
        assert d["total_fields"] == 5
        assert d["drifted_fields"] == ["env", "replicas"]
        assert d["has_drift"] is True


class TestSummarize:
    def test_none_raises(self):
        with pytest.raises(SummarizerError):
            summarize(None)

    def test_empty_list_returns_empty_report(self):
        report = summarize([])
        assert report.total_services == 0
        assert report.drifted_services == []
        assert report.clean_services == []

    def test_clean_service_recorded(self):
        result = _make("auth", spec={"replicas": 2})
        report = summarize([result])
        assert report.total_services == 1
        assert len(report.clean_services) == 1
        assert len(report.drifted_services) == 0

    def test_drifted_service_recorded(self):
        result = _make("auth", spec={"replicas": 2}, diffs={"replicas": (2, 3)})
        report = summarize([result])
        assert len(report.drifted_services) == 1
        assert report.drifted_services[0].service == "auth"

    def test_drifted_fields_captured(self):
        result = _make("svc", spec={"a": 1, "b": 2}, diffs={"a": (1, 9), "b": (2, 8)})
        report = summarize([result])
        assert set(report.services[0].drifted_fields) == {"a", "b"}

    def test_total_fields_reflects_spec_size(self):
        result = _make("svc", spec={"x": 1, "y": 2, "z": 3})
        report = summarize([result])
        assert report.services[0].total_fields == 3

    def test_text_output_contains_service_name(self):
        result = _make("payments", spec={"replicas": 1})
        report = summarize([result])
        assert "payments" in report.text()

    def test_text_output_shows_drift_label(self):
        result = _make("payments", spec={"replicas": 1}, diffs={"replicas": (1, 5)})
        report = summarize([result])
        assert "DRIFT" in report.text()

    def test_text_output_shows_ok_label(self):
        result = _make("clean-svc", spec={"replicas": 1})
        report = summarize([result])
        assert "OK" in report.text()

    def test_multiple_services_summary_counts(self):
        results = [
            _make("a", spec={"k": 1}),
            _make("b", spec={"k": 1}, diffs={"k": (1, 2)}),
            _make("c", spec={"k": 1}, diffs={"k": (1, 3)}),
        ]
        report = summarize(results)
        assert report.total_services == 3
        assert len(report.drifted_services) == 2
        assert len(report.clean_services) == 1
