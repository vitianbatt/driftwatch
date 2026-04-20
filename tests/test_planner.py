"""Tests for driftwatch.planner."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.planner import PlannerError, RemediationPlan, WorkItem, build_plan
from driftwatch.scorer import ScoredResult


def _diff(field: str, diff_type: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, expected="a", actual="b", diff_type=diff_type)


def _make(service: str, score: float, *fields: str) -> ScoredResult:
    diffs = [_diff(f) for f in fields]
    result = DriftResult(service=service, diffs=diffs)
    return ScoredResult(result=result, score=score, diffs=diffs)


class TestBuildPlan:
    def test_none_raises(self):
        with pytest.raises(PlannerError):
            build_plan(None)

    def test_empty_list_returns_empty_plan(self):
        plan = build_plan([])
        assert plan.total == 0

    def test_zero_score_excluded(self):
        r = _make("svc", 0.0)
        plan = build_plan([r])
        assert plan.total == 0

    def test_positive_score_included(self):
        r = _make("svc", 5.0, "replicas")
        plan = build_plan([r])
        assert plan.total == 1

    def test_sorted_by_score_descending(self):
        r1 = _make("low", 2.0, "a")
        r2 = _make("high", 15.0, "b")
        r3 = _make("mid", 7.0, "c")
        plan = build_plan([r1, r2, r3])
        assert [i.service for i in plan.items] == ["high", "mid", "low"]

    def test_priority_critical(self):
        r = _make("svc", 25.0, "x")
        plan = build_plan([r])
        assert plan.items[0].priority == "critical"

    def test_priority_high(self):
        r = _make("svc", 12.0, "x")
        plan = build_plan([r])
        assert plan.items[0].priority == "high"

    def test_priority_normal(self):
        r = _make("svc", 6.0, "x")
        plan = build_plan([r])
        assert plan.items[0].priority == "normal"

    def test_priority_low(self):
        r = _make("svc", 1.0, "x")
        plan = build_plan([r])
        assert plan.items[0].priority == "low"

    def test_fields_captured(self):
        r = _make("svc", 8.0, "cpu", "memory")
        plan = build_plan([r])
        assert sorted(plan.items[0].fields) == ["cpu", "memory"]


class TestRemediationPlan:
    def test_summary_empty(self):
        plan = RemediationPlan(items=[])
        assert plan.summary() == "No remediation required."

    def test_summary_lists_priorities(self):
        items = [
            WorkItem(service="a", score=25.0, priority="critical", fields=["x"]),
            WorkItem(service="b", score=5.0, priority="normal", fields=["y"]),
        ]
        plan = RemediationPlan(items=items)
        text = plan.summary()
        assert "CRITICAL" in text
        assert "NORMAL" in text
        assert "a" in text

    def test_by_priority_filters_correctly(self):
        items = [
            WorkItem(service="a", score=25.0, priority="critical", fields=[]),
            WorkItem(service="b", score=3.0, priority="low", fields=[]),
        ]
        plan = RemediationPlan(items=items)
        assert len(plan.by_priority("critical")) == 1
        assert len(plan.by_priority("low")) == 1
        assert len(plan.by_priority("high")) == 0

    def test_work_item_to_dict(self):
        item = WorkItem(service="svc", score=7.5, priority="normal", fields=["b", "a"])
        d = item.to_dict()
        assert d["service"] == "svc"
        assert d["score"] == 7.5
        assert d["fields"] == ["a", "b"]  # sorted
