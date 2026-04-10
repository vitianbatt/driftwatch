"""Tests for driftwatch.dispatcher."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.dispatcher import (
    DispatchReport,
    DispatchRule,
    DispatcherError,
    dispatch,
)


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# DispatchRule validation
# ---------------------------------------------------------------------------

class TestDispatchRule:
    def test_valid_rule_created(self):
        rule = DispatchRule(name="log", handler=lambda r: None)
        assert rule.name == "log"

    def test_empty_name_raises(self):
        with pytest.raises(DispatcherError, match="non-empty"):
            DispatchRule(name="", handler=lambda r: None)

    def test_whitespace_name_raises(self):
        with pytest.raises(DispatcherError, match="non-empty"):
            DispatchRule(name="   ", handler=lambda r: None)

    def test_non_callable_handler_raises(self):
        with pytest.raises(DispatcherError, match="callable"):
            DispatchRule(name="x", handler="not_a_function")  # type: ignore

    def test_non_callable_predicate_raises(self):
        with pytest.raises(DispatcherError, match="callable"):
            DispatchRule(name="x", handler=lambda r: None, predicate="bad")  # type: ignore


# ---------------------------------------------------------------------------
# DispatchReport helpers
# ---------------------------------------------------------------------------

class TestDispatchReport:
    def test_total_dispatched_empty(self):
        r = DispatchReport()
        assert r.total_dispatched() == 0

    def test_total_dispatched_counts_all(self):
        r = DispatchReport(dispatched={"log": ["svc-a", "svc-b"], "alert": ["svc-c"]})
        assert r.total_dispatched() == 3

    def test_summary_empty(self):
        assert DispatchReport().summary() == "No results dispatched."

    def test_summary_lists_handlers(self):
        r = DispatchReport(dispatched={"log": ["svc-a"]}, skipped=["svc-b"])
        out = r.summary()
        assert "log: 1" in out
        assert "skipped: 1" in out


# ---------------------------------------------------------------------------
# dispatch()
# ---------------------------------------------------------------------------

class TestDispatch:
    def test_none_results_raises(self):
        with pytest.raises(DispatcherError):
            dispatch(None, [])  # type: ignore

    def test_none_rules_raises(self):
        with pytest.raises(DispatcherError):
            dispatch([], None)  # type: ignore

    def test_empty_inputs_returns_empty_report(self):
        report = dispatch([], [])
        assert report.total_dispatched() == 0
        assert report.skipped == []

    def test_result_dispatched_to_matching_rule(self):
        received = []
        rule = DispatchRule(name="catch_all", handler=received.append)
        report = dispatch([_make("svc-a")], [rule])
        assert len(received) == 1
        assert report.dispatched["catch_all"] == ["svc-a"]

    def test_unmatched_result_goes_to_skipped(self):
        rule = DispatchRule(
            name="drift_only",
            handler=lambda r: None,
            predicate=lambda r: bool(r.diffs),
        )
        report = dispatch([_make("clean-svc")], [rule])
        assert "clean-svc" in report.skipped
        assert report.total_dispatched() == 0

    def test_result_dispatched_to_multiple_matching_rules(self):
        calls: list[str] = []
        r1 = DispatchRule(name="rule1", handler=lambda r: calls.append("r1"))
        r2 = DispatchRule(name="rule2", handler=lambda r: calls.append("r2"))
        report = dispatch([_make("svc")], [r1, r2])
        assert calls == ["r1", "r2"]
        assert report.total_dispatched() == 2

    def test_predicate_receives_correct_result(self):
        seen = []
        rule = DispatchRule(
            name="spy",
            handler=lambda r: None,
            predicate=lambda r: seen.append(r.service) or True,
        )
        dispatch([_make("my-svc")], [rule])
        assert seen == ["my-svc"]
