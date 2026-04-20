"""Tests for driftwatch.capper_cli."""

from __future__ import annotations

import json

import pytest

from driftwatch.capper_cli import results_from_json, report_to_json, run_capper
from driftwatch.capper import CapConfig, cap_results
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _make(service: str, n_diffs: int = 0) -> DriftResult:
    diffs = [
        FieldDiff(field=f"field_{i}", expected="a", actual="b", kind="changed")
        for i in range(n_diffs)
    ]
    return DriftResult(service=service, diffs=diffs)


_RAW_CLEAN = {"service": "auth", "diffs": []}
_RAW_DRIFT = {
    "service": "billing",
    "diffs": [
        {"field": "timeout", "expected": "30", "actual": "60", "kind": "changed"},
        {"field": "replicas", "expected": "3", "actual": "1", "kind": "changed"},
    ],
}


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        results = results_from_json([_RAW_CLEAN])
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        results = results_from_json([_RAW_DRIFT])
        assert len(results[0].diffs) == 2

    def test_field_names_preserved(self):
        results = results_from_json([_RAW_DRIFT])
        fields = [d.field for d in results[0].diffs]
        assert fields == ["timeout", "replicas"]

    def test_multiple_results_parsed(self):
        results = results_from_json([_RAW_CLEAN, _RAW_DRIFT])
        assert len(results) == 2


class TestReportToJson:
    def test_output_is_valid_json(self):
        result = _make("svc", 2)
        config = CapConfig(max_diffs=5)
        capped = cap_results([result], config)
        out = report_to_json(capped)
        parsed = json.loads(out)
        assert isinstance(parsed, list)

    def test_service_name_present(self):
        result = _make("gateway", 1)
        config = CapConfig(max_diffs=5)
        capped = cap_results([result], config)
        out = json.loads(report_to_json(capped))
        assert out[0]["service"] == "gateway"

    def test_was_capped_false_when_under_limit(self):
        result = _make("svc", 2)
        config = CapConfig(max_diffs=5)
        capped = cap_results([result], config)
        out = json.loads(report_to_json(capped))
        assert out[0]["capped"] is False

    def test_was_capped_true_when_over_limit(self):
        result = _make("svc", 6)
        config = CapConfig(max_diffs=3)
        capped = cap_results([result], config)
        out = json.loads(report_to_json(capped))
        assert out[0]["capped"] is True
        assert out[0]["shown"] == 3
        assert out[0]["total"] == 6


class TestRunCapper:
    def test_end_to_end_clean(self):
        out = json.loads(run_capper([_RAW_CLEAN]))
        assert out[0]["has_drift"] is False

    def test_end_to_end_drift(self):
        out = json.loads(run_capper([_RAW_DRIFT], max_diffs=1))
        assert out[0]["capped"] is True
        assert out[0]["shown"] == 1

    def test_custom_max_diffs_respected(self):
        raw = {
            "service": "x",
            "diffs": [
                {"field": f"f{i}", "expected": "a", "actual": "b", "kind": "changed"}
                for i in range(10)
            ],
        }
        out = json.loads(run_capper([raw], max_diffs=4))
        assert out[0]["shown"] == 4
        assert out[0]["total"] == 10
