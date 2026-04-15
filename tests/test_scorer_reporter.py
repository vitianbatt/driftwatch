"""Tests for driftwatch.scorer_reporter."""
from __future__ import annotations

import json
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport, build_scored_report
from driftwatch.scorer_reporter import (
    ReportFormat,
    ScorerReporterError,
    generate_scorer_report,
)


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="x", actual=None)


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _report(*services_with_diffs) -> ScoredReport:
    results = [_make(s, d) for s, d in services_with_diffs]
    return build_scored_report(results)


# ---------------------------------------------------------------------------
# generate_scorer_report — text
# ---------------------------------------------------------------------------

class TestGenerateScorerReportText:
    def test_none_report_raises(self):
        with pytest.raises(ScorerReporterError):
            generate_scorer_report(None)

    def test_empty_results_message(self):
        report = _report()
        out = generate_scorer_report(report)
        assert "No scored results" in out

    def test_ok_service_shown(self):
        report = _report(("auth", []))
        out = generate_scorer_report(report)
        assert "auth" in out
        assert "OK" in out

    def test_drift_service_shown(self):
        report = _report(("payments", [_diff("replicas")]))
        out = generate_scorer_report(report)
        assert "DRIFT" in out
        assert "payments" in out

    def test_field_names_listed_in_text(self):
        report = _report(("svc", [_diff("timeout"), _diff("memory")]))
        out = generate_scorer_report(report)
        assert "timeout" in out
        assert "memory" in out

    def test_average_score_in_header(self):
        report = _report(("a", [_diff("x")]), ("b", []))
        out = generate_scorer_report(report)
        assert "avg_score" in out


# ---------------------------------------------------------------------------
# generate_scorer_report — json
# ---------------------------------------------------------------------------

class TestGenerateScorerReportJson:
    def test_output_is_valid_json(self):
        report = _report(("auth", [_diff("env")]))
        raw = generate_scorer_report(report, fmt=ReportFormat.JSON)
        data = json.loads(raw)
        assert "results" in data
        assert "average_score" in data
        assert "total" in data

    def test_total_matches_results_length(self):
        report = _report(("a", []), ("b", [_diff("x")]))
        data = json.loads(generate_scorer_report(report, fmt=ReportFormat.JSON))
        assert data["total"] == 2

    def test_service_name_in_json_results(self):
        report = _report(("gateway", [_diff("port")]))
        data = json.loads(generate_scorer_report(report, fmt=ReportFormat.JSON))
        services = [r["service"] for r in data["results"]]
        assert "gateway" in services

    def test_unsupported_format_raises(self):
        report = _report(("svc", []))
        with pytest.raises(ScorerReporterError):
            generate_scorer_report(report, fmt="xml")  # type: ignore
