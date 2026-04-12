"""Tests for pruner_cli helpers."""
from __future__ import annotations

import json

import pytest

from driftwatch.pruner_cli import (
    config_from_dict,
    results_from_json,
    report_to_json,
    run_pruner,
)
from driftwatch.pruner import PruneConfig, PrunedReport
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _make(service: str, diffs: list[FieldDiff] | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="x", actual=None)


# ---------------------------------------------------------------------------
# config_from_dict
# ---------------------------------------------------------------------------

class TestConfigFromDict:
    def test_empty_dict_gives_defaults(self):
        cfg = config_from_dict({})
        assert cfg.max_age_days is None
        assert cfg.max_results is None
        assert cfg.drop_clean is False

    def test_max_results_parsed(self):
        cfg = config_from_dict({"max_results": 10})
        assert cfg.max_results == 10

    def test_drop_clean_parsed(self):
        cfg = config_from_dict({"drop_clean": True})
        assert cfg.drop_clean is True

    def test_max_age_days_parsed(self):
        cfg = config_from_dict({"max_age_days": 7})
        assert cfg.max_age_days == 7


# ---------------------------------------------------------------------------
# results_from_json
# ---------------------------------------------------------------------------

class TestResultsFromJson:
    def test_clean_result_parsed(self):
        payload = json.dumps([{"service": "auth", "diffs": []}])
        results = results_from_json(payload)
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        payload = json.dumps([
            {
                "service": "billing",
                "diffs": [{"field": "timeout", "kind": "missing", "expected": "30s", "actual": None}],
            }
        ])
        results = results_from_json(payload)
        assert results[0].diffs[0].field == "timeout"
        assert results[0].diffs[0].kind == "missing"

    def test_multiple_results_parsed(self):
        payload = json.dumps([
            {"service": "a", "diffs": []},
            {"service": "b", "diffs": []},
        ])
        assert len(results_from_json(payload)) == 2


# ---------------------------------------------------------------------------
# report_to_json
# ---------------------------------------------------------------------------

class TestReportToJson:
    def _build_report(self, kept, pruned_count=0):
        return PrunedReport(kept=kept, pruned_count=pruned_count)

    def test_output_is_valid_json(self):
        report = self._build_report([_make("svc")])
        data = json.loads(report_to_json(report))
        assert "kept" in data
        assert "total" in data
        assert "pruned_count" in data

    def test_total_reflects_kept_plus_pruned(self):
        report = self._build_report([_make("a"), _make("b")], pruned_count=3)
        data = json.loads(report_to_json(report))
        assert data["total"] == 5

    def test_diffs_serialised(self):
        result = _make("x", [_diff("port")])
        report = self._build_report([result])
        data = json.loads(report_to_json(report))
        assert data["kept"][0]["diffs"][0]["field"] == "port"


# ---------------------------------------------------------------------------
# run_pruner integration
# ---------------------------------------------------------------------------

def test_run_pruner_drop_clean_removes_clean_results():
    cfg = PruneConfig(drop_clean=True)
    results = [_make("clean"), _make("dirty", [_diff("f")])]
    report = run_pruner(cfg, results)
    assert all(r.diffs for r in report.kept)


def test_run_pruner_max_results_limits_output():
    cfg = PruneConfig(max_results=1)
    results = [_make("a", [_diff("f")]), _make("b", [_diff("g")])]
    report = run_pruner(cfg, results)
    assert len(report.kept) == 1
