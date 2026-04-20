"""Tests for driftwatch.scorer_threshold_cli."""
import json
import pytest
from driftwatch.scorer_threshold import ScorerThresholdError
from driftwatch.scorer_threshold_cli import (
    results_from_json,
    report_to_json,
    run_threshold,
)


_CLEAN = {"service": "svc-clean", "score": 0.0, "diffs": []}
_DRIFT_HIGH = {
    "service": "svc-high",
    "score": 9.0,
    "diffs": [{"field": "replicas", "kind": "changed", "expected": "2", "actual": "3"}],
}
_DRIFT_LOW = {
    "service": "svc-low",
    "score": 1.0,
    "diffs": [{"field": "image", "kind": "changed", "expected": "v1", "actual": "v2"}],
}


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        report = results_from_json([_CLEAN])
        assert len(report.results) == 1
        assert report.results[0].result.service == "svc-clean"

    def test_drift_result_diffs_parsed(self):
        report = results_from_json([_DRIFT_HIGH])
        assert len(report.results[0].result.diffs) == 1

    def test_score_parsed_correctly(self):
        report = results_from_json([_DRIFT_HIGH])
        assert report.results[0].score == 9.0

    def test_multiple_results_parsed(self):
        report = results_from_json([_CLEAN, _DRIFT_HIGH, _DRIFT_LOW])
        assert len(report.results) == 3


class TestReportToJson:
    def _run(self, raw, min_score=5.0):
        from driftwatch.scorer_threshold import ThresholdConfig, apply_threshold
        scored = results_from_json(raw)
        config = ThresholdConfig(min_score=min_score)
        report = apply_threshold(scored, config)
        return json.loads(report_to_json(report))

    def test_output_contains_threshold(self):
        data = self._run([_DRIFT_HIGH])
        assert data["threshold"] == 5.0

    def test_kept_services_listed(self):
        data = self._run([_DRIFT_HIGH, _DRIFT_LOW])
        assert data["total_kept"] == 1
        assert data["kept"][0]["service"] == "svc-high"

    def test_dropped_count_correct(self):
        data = self._run([_DRIFT_HIGH, _DRIFT_LOW])
        assert data["total_dropped"] == 1


class TestRunThreshold:
    def test_end_to_end_filters_correctly(self):
        output = json.loads(run_threshold([_DRIFT_HIGH, _DRIFT_LOW], min_score=5.0))
        assert output["total_kept"] == 1
        assert output["kept"][0]["service"] == "svc-high"

    def test_negative_threshold_raises(self):
        with pytest.raises(ScorerThresholdError):
            run_threshold([_DRIFT_HIGH], min_score=-1.0)

    def test_include_clean_keeps_clean_results(self):
        output = json.loads(
            run_threshold([_CLEAN, _DRIFT_HIGH], min_score=0.0, include_clean=True)
        )
        services = [r["service"] for r in output["kept"]]
        assert "svc-clean" in services
