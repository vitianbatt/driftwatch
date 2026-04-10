"""Tests for driftwatch.patcher_cli."""
import json

import pytest

from driftwatch.patcher_cli import results_from_json, report_to_json, run_patcher
from driftwatch.patcher import PatchReport, PatchSuggestion


_CLEAN_JSON = json.dumps([{"service": "auth", "diffs": []}])
_DRIFT_JSON = json.dumps([
    {
        "service": "billing",
        "diffs": [
            {"field": "replicas", "expected": 3, "actual": None},
            {"field": "timeout", "expected": 60, "actual": 30},
        ],
    }
])


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        results = results_from_json(_CLEAN_JSON)
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        results = results_from_json(_DRIFT_JSON)
        assert len(results[0].diffs) == 2

    def test_field_names_preserved(self):
        results = results_from_json(_DRIFT_JSON)
        fields = [d.field for d in results[0].diffs]
        assert "replicas" in fields
        assert "timeout" in fields


class TestReportToJson:
    def test_output_is_valid_json(self):
        report = PatchReport(
            suggestions=[
                PatchSuggestion("svc", "k", "set", expected=1, actual=None)
            ]
        )
        out = report_to_json(report)
        parsed = json.loads(out)
        assert parsed["total"] == 1
        assert len(parsed["suggestions"]) == 1

    def test_empty_report_json(self):
        out = json.loads(report_to_json(PatchReport()))
        assert out["total"] == 0
        assert out["suggestions"] == []


class TestRunPatcher:
    def test_text_output_no_patches(self):
        result = run_patcher(_CLEAN_JSON, output_format="text")
        assert "No patches" in result

    def test_text_output_with_patches(self):
        result = run_patcher(_DRIFT_JSON, output_format="text")
        assert "patch suggestion" in result
        assert "billing" in result

    def test_json_output_structure(self):
        result = run_patcher(_DRIFT_JSON, output_format="json")
        parsed = json.loads(result)
        assert "total" in parsed
        assert parsed["total"] == 2

    def test_default_format_is_text(self):
        result = run_patcher(_CLEAN_JSON)
        assert isinstance(result, str)
