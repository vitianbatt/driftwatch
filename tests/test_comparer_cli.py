"""Tests for driftwatch.comparer_cli."""

from __future__ import annotations

import json

import pytest

from driftwatch.comparer_cli import results_from_json, report_to_json
from driftwatch.baseline_comparator import BaselineDriftReport


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _raw_clean():
    return [{"service": "auth", "diffs": []}]


def _raw_drift():
    return [
        {
            "service": "payments",
            "diffs": [
                {"field": "replicas", "expected": 3, "actual": 1, "diff_type": "changed"},
                {"field": "image", "expected": "v2", "actual": None, "diff_type": "missing"},
            ],
        }
    ]


# ---------------------------------------------------------------------------
# results_from_json
# ---------------------------------------------------------------------------

class TestResultsFromJson:
    def test_clean_result_parsed(self):
        results = results_from_json(_raw_clean())
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        results = results_from_json(_raw_drift())
        assert len(results) == 1
        assert len(results[0].diffs) == 2

    def test_field_names_preserved(self):
        results = results_from_json(_raw_drift())
        fields = [d.field for d in results[0].diffs]
        assert "replicas" in fields
        assert "image" in fields

    def test_empty_list_returns_empty(self):
        assert results_from_json([]) == []

    def test_diff_type_defaults_to_changed(self):
        raw = [{"service": "svc", "diffs": [{"field": "x", "expected": 1, "actual": 2}]}]
        results = results_from_json(raw)
        assert results[0].diffs[0].diff_type == "changed"


# ---------------------------------------------------------------------------
# report_to_json
# ---------------------------------------------------------------------------

def _make_entry(service: str, has_drift: bool, fields: list[str]):
    from driftwatch.baseline_comparator import BaselineDriftEntry
    return BaselineDriftEntry(service=service, has_drift=has_drift, drift_fields=fields)


class TestReportToJson:
    def test_output_is_valid_json(self):
        report = BaselineDriftReport(entries=[])
        text = report_to_json(report)
        parsed = json.loads(text)
        assert isinstance(parsed, dict)

    def test_no_drift_flag_when_clean(self):
        report = BaselineDriftReport(entries=[_make_entry("auth", False, [])])
        parsed = json.loads(report_to_json(report))
        assert parsed["has_drift"] is False

    def test_drift_flag_when_drifted(self):
        report = BaselineDriftReport(entries=[_make_entry("payments", True, ["replicas"])])
        parsed = json.loads(report_to_json(report))
        assert parsed["has_drift"] is True

    def test_services_list_populated(self):
        report = BaselineDriftReport(
            entries=[
                _make_entry("auth", False, []),
                _make_entry("payments", True, ["replicas"]),
            ]
        )
        parsed = json.loads(report_to_json(report))
        service_names = [s["service"] for s in parsed["services"]]
        assert "auth" in service_names
        assert "payments" in service_names

    def test_summary_key_present(self):
        report = BaselineDriftReport(entries=[])
        parsed = json.loads(report_to_json(report))
        assert "summary" in parsed
