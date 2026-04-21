"""Tests for driftwatch.scorer_merger_cli."""
import json
import pytest

from driftwatch.scorer_merger_cli import results_from_json, report_to_json, run_merger
from driftwatch.scorer_merger import MergedScoredReport, MergedScoredResult


_CLEAN = {"service": "auth", "score": 0.0, "drifted_fields": [], "diffs": []}
_DRIFT = {
    "service": "billing",
    "score": 3.0,
    "drifted_fields": ["env", "port"],
    "diffs": [
        {"field": "env", "kind": "missing", "expected": "prod", "actual": None},
        {"field": "port", "kind": "changed", "expected": "8080", "actual": "9090"},
    ],
}


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        report = results_from_json([_CLEAN])
        assert len(report.results) == 1
        assert report.results[0].service == "auth"
        assert report.results[0].score == 0.0

    def test_drift_result_diffs_parsed(self):
        report = results_from_json([_DRIFT])
        r = report.results[0]
        assert r.score == 3.0
        assert r.drifted_fields == ["env", "port"]
        assert len(r.result.diffs) == 2

    def test_multiple_results_parsed(self):
        report = results_from_json([_CLEAN, _DRIFT])
        assert len(report.results) == 2

    def test_empty_list_returns_empty_report(self):
        report = results_from_json([])
        assert len(report.results) == 0


class TestReportToJson:
    def _make_report(self) -> MergedScoredReport:
        return MergedScoredReport(
            results=[
                MergedScoredResult(service="auth", score=0.0, source="primary"),
                MergedScoredResult(service="billing", score=3.0, drifted_fields=["env"], source="secondary", conflict=True),
            ],
            conflict_count=1,
        )

    def test_returns_valid_json(self):
        out = report_to_json(self._make_report())
        parsed = json.loads(out)
        assert isinstance(parsed, dict)

    def test_total_present(self):
        parsed = json.loads(report_to_json(self._make_report()))
        assert parsed["total"] == 2

    def test_conflict_count_present(self):
        parsed = json.loads(report_to_json(self._make_report()))
        assert parsed["conflict_count"] == 1

    def test_average_score_present(self):
        parsed = json.loads(report_to_json(self._make_report()))
        assert "average_score" in parsed

    def test_results_list_present(self):
        parsed = json.loads(report_to_json(self._make_report()))
        assert len(parsed["results"]) == 2


class TestRunMerger:
    def test_merge_distinct_services(self):
        primary = [_CLEAN]
        secondary = [_DRIFT]
        out = json.loads(run_merger(primary, secondary))
        assert out["total"] == 2
        assert out["conflict_count"] == 0

    def test_merge_conflict_detected(self):
        p = [{"service": "auth", "score": 5.0, "drifted_fields": ["env"], "diffs": [{"field": "env", "kind": "missing", "expected": "x", "actual": None}]}]
        s = [{"service": "auth", "score": 2.0, "drifted_fields": ["env"], "diffs": [{"field": "env", "kind": "missing", "expected": "x", "actual": None}]}]
        out = json.loads(run_merger(p, s))
        assert out["conflict_count"] == 1
        assert out["results"][0]["score"] == 5.0

    def test_empty_both_returns_zero_total(self):
        out = json.loads(run_merger([], []))
        assert out["total"] == 0
