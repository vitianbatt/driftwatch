"""Tests for driftwatch.reporter module."""

import json

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.reporter import OutputFormat, ReportError, generate_report


def _make_result(name: str, missing=None, mismatched=None) -> DriftResult:
    return DriftResult(
        service_name=name,
        missing_keys=set(missing or []),
        mismatched_keys=dict(mismatched or {}),
    )


class TestGenerateReportText:
    def test_no_results_returns_no_drift_message(self):
        report = generate_report([], fmt=OutputFormat.TEXT)
        assert "No drift" in report

    def test_clean_service_listed(self):
        result = _make_result("auth-service")
        report = generate_report([result], fmt=OutputFormat.TEXT)
        assert "auth-service" in report
        assert "No drift" in report

    def test_missing_key_shown_in_text(self):
        result = _make_result("payments", missing=["DB_HOST"])
        report = generate_report([result], fmt=OutputFormat.TEXT)
        assert "MISSING" in report
        assert "DB_HOST" in report

    def test_mismatched_key_shown_in_text(self):
        result = _make_result(
            "gateway",
            mismatched={"REPLICAS": (3, 1)},
        )
        report = generate_report([result], fmt=OutputFormat.TEXT)
        assert "MISMATCH" in report
        assert "REPLICAS" in report
        assert "expected=3" in report
        assert "actual=1" in report

    def test_drifted_service_count_in_header(self):
        r1 = _make_result("svc-a", missing=["X"])
        r2 = _make_result("svc-b", missing=["Y"])
        report = generate_report([r1, r2], fmt=OutputFormat.TEXT)
        assert "2 service(s)" in report

    def test_mixed_results_shows_both_sections(self):
        drifted = _make_result("svc-bad", missing=["KEY"])
        clean = _make_result("svc-good")
        report = generate_report([drifted, clean], fmt=OutputFormat.TEXT)
        assert "svc-bad" in report
        assert "svc-good" in report
        assert "No drift" in report


class TestGenerateReportJson:
    def test_returns_valid_json(self):
        result = _make_result("api", missing=["PORT"])
        raw = generate_report([result], fmt=OutputFormat.JSON)
        parsed = json.loads(raw)
        assert isinstance(parsed, list)

    def test_json_has_drift_flag(self):
        result = _make_result("api", missing=["PORT"])
        parsed = json.loads(generate_report([result], fmt=OutputFormat.JSON))
        assert parsed[0]["has_drift"] is True

    def test_json_no_drift_flag(self):
        result = _make_result("api")
        parsed = json.loads(generate_report([result], fmt=OutputFormat.JSON))
        assert parsed[0]["has_drift"] is False

    def test_json_missing_keys_present(self):
        result = _make_result("svc", missing=["A", "B"])
        parsed = json.loads(generate_report([result], fmt=OutputFormat.JSON))
        assert set(parsed[0]["missing_keys"]) == {"A", "B"}

    def test_json_mismatched_keys_structure(self):
        result = _make_result("svc", mismatched={"TIMEOUT": (30, 60)})
        parsed = json.loads(generate_report([result], fmt=OutputFormat.JSON))
        mm = parsed[0]["mismatched_keys"]["TIMEOUT"]
        assert mm["expected"] == 30
        assert mm["actual"] == 60

    def test_empty_results_returns_empty_list(self):
        parsed = json.loads(generate_report([], fmt=OutputFormat.JSON))
        assert parsed == []


def test_unsupported_format_raises_report_error():
    with pytest.raises(ReportError):
        generate_report([], fmt="xml")  # type: ignore[arg-type]
