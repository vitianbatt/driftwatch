"""Tests for driftwatch/scorer_cli.py."""
from __future__ import annotations

import json
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredReport
from driftwatch.scorer_cli import report_to_json, results_from_json, run_scorer


def _make(service: str, diffs: list[FieldDiff] | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        raw = [{"service": "auth", "diffs": []}]
        results = results_from_json(raw)
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        raw = [
            {
                "service": "api",
                "diffs": [
                    {
                        "field": "replicas",
                        "diff_type": "changed",
                        "expected": 3,
                        "actual": 1,
                    }
                ],
            }
        ]
        results = results_from_json(raw)
        assert len(results[0].diffs) == 1
        assert results[0].diffs[0].field == "replicas"

    def test_multiple_results_parsed(self):
        raw = [
            {"service": "svc-a", "diffs": []},
            {"service": "svc-b", "diffs": []},
        ]
        assert len(results_from_json(raw)) == 2

    def test_field_names_preserved(self):
        raw = [
            {
                "service": "db",
                "diffs": [
                    {"field": "timeout", "diff_type": "missing", "expected": 30}
                ],
            }
        ]
        results = results_from_json(raw)
        assert results[0].diffs[0].field == "timeout"
        assert results[0].diffs[0].diff_type == "missing"


class TestReportToJson:
    def test_output_is_valid_json(self):
        raw = [{"service": "auth", "diffs": []}]
        report = run_scorer(raw)
        output = report_to_json(report)
        parsed = json.loads(output)
        assert "average_score" in parsed
        assert "results" in parsed

    def test_average_score_present(self):
        raw = [{"service": "auth", "diffs": []}]
        report = run_scorer(raw)
        parsed = json.loads(report_to_json(report))
        assert isinstance(parsed["average_score"], (int, float))

    def test_results_list_matches_input(self):
        raw = [
            {"service": "a", "diffs": []},
            {"service": "b", "diffs": []},
        ]
        report = run_scorer(raw)
        parsed = json.loads(report_to_json(report))
        assert len(parsed["results"]) == 2
