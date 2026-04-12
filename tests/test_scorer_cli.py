"""Tests for scorer_cli helpers."""
from __future__ import annotations

import json

import pytest

from driftwatch.scorer_cli import results_from_json, report_to_json, run_scorer
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import score_results


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [FieldDiff(field=f, expected="a", actual="b") for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        raw = json.dumps([{"service": "auth", "diffs": []}])
        results = results_from_json(raw)
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        raw = json.dumps([
            {"service": "api", "diffs": [{"field": "replicas", "expected": "3", "actual": "1"}]}
        ])
        results = results_from_json(raw)
        assert results[0].diffs[0].field == "replicas"

    def test_multiple_results_parsed(self):
        raw = json.dumps([
            {"service": "svc-a", "diffs": []},
            {"service": "svc-b", "diffs": [{"field": "x", "expected": "1", "actual": "2"}]},
        ])
        results = results_from_json(raw)
        assert len(results) == 2


class TestReportToJson:
    def test_output_is_valid_json(self):
        report = score_results([_make("auth")])
        out = report_to_json(report)
        parsed = json.loads(out)
        assert isinstance(parsed, list)

    def test_each_entry_has_required_keys(self):
        report = score_results([_make("auth", ["replicas"])])
        out = json.loads(report_to_json(report))
        entry = out[0]
        assert "service" in entry
        assert "score" in entry
        assert "priority" in entry
        assert "drift_fields" in entry

    def test_score_reflects_drift(self):
        report = score_results([_make("auth", ["replicas", "image"])])
        out = json.loads(report_to_json(report))
        assert out[0]["score"] > 0


class TestRunScorer:
    def test_end_to_end_clean(self):
        raw = json.dumps([{"service": "auth", "diffs": []}])
        out = json.loads(run_scorer(raw))
        assert out[0]["service"] == "auth"
        assert out[0]["score"] == 0

    def test_end_to_end_with_drift(self):
        raw = json.dumps([
            {"service": "api", "diffs": [{"field": "replicas", "expected": "3", "actual": "1"}]}
        ])
        out = json.loads(run_scorer(raw))
        assert out[0]["score"] > 0
