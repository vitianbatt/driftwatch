"""Tests for driftwatch.planner_cli."""
from __future__ import annotations

import json

import pytest

from driftwatch.planner_cli import plan_to_json, results_from_json, run_planner
from driftwatch.planner import RemediationPlan, WorkItem


def _raw(service: str, diffs: list[dict] | None = None) -> dict:
    return {"service": service, "diffs": diffs or []}


class TestResultsFromJson:
    def test_clean_result_parsed(self):
        results = results_from_json([_raw("svc-a")])
        assert len(results) == 1
        assert results[0].service == "svc-a"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        raw = _raw("svc-b", [{"field": "replicas", "expected": "3", "actual": "1", "diff_type": "changed"}])
        results = results_from_json([raw])
        assert len(results[0].diffs) == 1
        assert results[0].diffs[0].field == "replicas"

    def test_multiple_results_parsed(self):
        raws = [_raw("a"), _raw("b"), _raw("c")]
        results = results_from_json(raws)
        assert [r.service for r in results] == ["a", "b", "c"]


class TestPlanToJson:
    def test_empty_plan_serialises(self):
        plan = RemediationPlan(items=[])
        output = json.loads(plan_to_json(plan))
        assert output["total"] == 0
        assert output["items"] == []

    def test_plan_with_items_serialises(self):
        items = [
            WorkItem(service="svc", score=12.0, priority="high", fields=["cpu"]),
        ]
        plan = RemediationPlan(items=items)
        output = json.loads(plan_to_json(plan))
        assert output["total"] == 1
        assert output["items"][0]["service"] == "svc"
        assert output["items"][0]["priority"] == "high"


class TestRunPlanner:
    def test_clean_services_produce_empty_plan(self):
        raws = [_raw("svc-a"), _raw("svc-b")]
        output = json.loads(run_planner(raws))
        assert output["total"] == 0

    def test_drifted_service_appears_in_plan(self):
        raws = [
            _raw("svc-x", [{"field": "image", "expected": "v1", "actual": "v2", "diff_type": "changed"}]),
        ]
        output = json.loads(run_planner(raws))
        assert output["total"] >= 1
        assert output["items"][0]["service"] == "svc-x"

    def test_custom_weights_influence_plan(self):
        raws = [
            _raw("svc-a", [{"field": "image", "expected": "v1", "actual": "v2", "diff_type": "changed"}]),
            _raw("svc-b", [{"field": "replicas", "expected": "3", "actual": "1", "diff_type": "changed"}]),
        ]
        output = json.loads(run_planner(raws, weights={"replicas": 50.0}))
        services = [i["service"] for i in output["items"]]
        assert services[0] == "svc-b"  # replicas weighted highest
