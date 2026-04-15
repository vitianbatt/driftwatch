"""Tests for driftwatch.drift_summary_reporter."""
from __future__ import annotations

import json

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.drift_summary_reporter import (
    DriftSummaryReport,
    DriftSummaryReporterError,
    SummaryFormat,
    build_summary,
    generate_summary_report,
)


def _make(service: str, *, drifted: bool = False) -> DriftResult:
    diffs = [
        FieldDiff(field="replicas", expected=3, actual=1, diff_type="changed")
    ] if drifted else []
    return DriftResult(service=service, diffs=diffs)


class TestBuildSummary:
    def test_empty_list_returns_zeros(self):
        report = build_summary([])
        assert report.total == 0
        assert report.drifted == 0
        assert report.clean == 0

    def test_none_raises(self):
        with pytest.raises(DriftSummaryReporterError):
            build_summary(None)  # type: ignore[arg-type]

    def test_all_clean_services(self):
        results = [_make("svc-a"), _make("svc-b")]
        report = build_summary(results)
        assert report.total == 2
        assert report.drifted == 0
        assert report.clean == 2
        assert report.drifted_services == []

    def test_all_drifted_services(self):
        results = [_make("svc-a", drifted=True), _make("svc-b", drifted=True)]
        report = build_summary(results)
        assert report.drifted == 2
        assert report.clean == 0

    def test_mixed_services(self):
        results = [_make("svc-a", drifted=True), _make("svc-b"), _make("svc-c", drifted=True)]
        report = build_summary(results)
        assert report.total == 3
        assert report.drifted == 2
        assert report.clean == 1

    def test_drifted_services_sorted(self):
        results = [_make("zebra", drifted=True), _make("alpha", drifted=True)]
        report = build_summary(results)
        assert report.drifted_services == ["alpha", "zebra"]

    def test_drift_rate_zero_when_no_results(self):
        assert build_summary([]).drift_rate == 0.0

    def test_drift_rate_calculated(self):
        results = [_make("a", drifted=True), _make("b"), _make("c"), _make("d")]
        report = build_summary(results)
        assert report.drift_rate == 0.25

    def test_to_dict_contains_all_keys(self):
        report = build_summary([_make("svc", drifted=True)])
        d = report.to_dict()
        assert set(d.keys()) == {"total", "drifted", "clean", "drift_rate", "drifted_services"}


class TestGenerateSummaryReport:
    def test_text_format_default(self):
        results = [_make("svc-a", drifted=True), _make("svc-b")]
        output = generate_summary_report(results)
        assert "Total services checked" in output
        assert "svc-a" in output

    def test_json_format_valid(self):
        results = [_make("svc-a", drifted=True)]
        output = generate_summary_report(results, fmt=SummaryFormat.JSON)
        parsed = json.loads(output)
        assert parsed["total"] == 1
        assert parsed["drifted"] == 1

    def test_no_drift_text_omits_drifted_list(self):
        results = [_make("svc-a"), _make("svc-b")]
        output = generate_summary_report(results)
        assert "Drifted services" not in output

    def test_drift_rate_shown_as_percentage(self):
        results = [_make("a", drifted=True), _make("b")]
        output = generate_summary_report(results)
        assert "50.0%" in output
