"""Tests for driftwatch/evaluator.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.evaluator import (
    EvaluatedResult,
    EvaluatorError,
    ThresholdRule,
    evaluate_results,
)


def _diff(field: str = "replicas", expected="2", actual="3") -> FieldDiff:
    return FieldDiff(field=field, expected=expected, actual=actual, diff_type="changed")


def _make(service: str = "svc", diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


class TestThresholdRule:
    def test_valid_rule_created(self):
        rule = ThresholdRule(name="warn", min_drift_fields=2)
        assert rule.name == "warn"
        assert rule.min_drift_fields == 2
        assert rule.tag == "threshold-breach"

    def test_custom_tag_accepted(self):
        rule = ThresholdRule(name="critical", min_drift_fields=5, tag="critical-breach")
        assert rule.tag == "critical-breach"

    def test_empty_name_raises(self):
        with pytest.raises(EvaluatorError, match="name"):
            ThresholdRule(name="", min_drift_fields=1)

    def test_whitespace_name_raises(self):
        with pytest.raises(EvaluatorError, match="name"):
            ThresholdRule(name="   ", min_drift_fields=1)

    def test_zero_min_drift_fields_raises(self):
        with pytest.raises(EvaluatorError, match="min_drift_fields"):
            ThresholdRule(name="rule", min_drift_fields=0)

    def test_negative_min_drift_fields_raises(self):
        with pytest.raises(EvaluatorError, match="min_drift_fields"):
            ThresholdRule(name="rule", min_drift_fields=-1)

    def test_empty_tag_raises(self):
        with pytest.raises(EvaluatorError, match="tag"):
            ThresholdRule(name="rule", min_drift_fields=1, tag="")


class TestEvaluatedResult:
    def test_no_breach_when_no_triggered(self):
        er = EvaluatedResult(result=_make())
        assert not er.has_breach()

    def test_has_breach_when_triggered(self):
        rule = ThresholdRule(name="warn", min_drift_fields=1)
        er = EvaluatedResult(result=_make(diffs=[_diff()]), triggered=[rule])
        assert er.has_breach()

    def test_breach_names_empty_when_no_triggered(self):
        er = EvaluatedResult(result=_make())
        assert er.breach_names() == []

    def test_breach_names_lists_rule_names(self):
        r1 = ThresholdRule(name="warn", min_drift_fields=1)
        r2 = ThresholdRule(name="critical", min_drift_fields=3)
        er = EvaluatedResult(result=_make(diffs=[_diff()]), triggered=[r1, r2])
        assert er.breach_names() == ["warn", "critical"]

    def test_to_dict_contains_expected_keys(self):
        er = EvaluatedResult(result=_make(service="auth", diffs=[_diff()]))
        d = er.to_dict()
        assert set(d.keys()) == {
            "service", "has_drift", "drift_field_count", "has_breach", "triggered_rules"
        }

    def test_to_dict_drift_field_count(self):
        er = EvaluatedResult(result=_make(diffs=[_diff(), _diff("image")]))
        assert er.to_dict()["drift_field_count"] == 2


class TestEvaluateResults:
    def test_none_results_raises(self):
        with pytest.raises(EvaluatorError):
            evaluate_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(EvaluatorError):
            evaluate_results([], None)

    def test_empty_results_returns_empty(self):
        assert evaluate_results([], []) == []

    def test_no_rules_no_breach(self):
        results = [_make(diffs=[_diff()])]
        evaluated = evaluate_results(results, [])
        assert len(evaluated) == 1
        assert not evaluated[0].has_breach()

    def test_breach_triggered_when_threshold_met(self):
        rule = ThresholdRule(name="warn", min_drift_fields=2)
        results = [_make(diffs=[_diff(), _diff("image")])]
        evaluated = evaluate_results(results, [rule])
        assert evaluated[0].has_breach()
        assert evaluated[0].breach_names() == ["warn"]

    def test_no_breach_when_below_threshold(self):
        rule = ThresholdRule(name="warn", min_drift_fields=3)
        results = [_make(diffs=[_diff()])]
        evaluated = evaluate_results(results, [rule])
        assert not evaluated[0].has_breach()

    def test_multiple_rules_only_matching_triggered(self):
        low = ThresholdRule(name="low", min_drift_fields=1)
        high = ThresholdRule(name="high", min_drift_fields=5)
        results = [_make(diffs=[_diff(), _diff("image")])]
        evaluated = evaluate_results(results, [low, high])
        assert evaluated[0].breach_names() == ["low"]

    def test_clean_result_not_breached(self):
        rule = ThresholdRule(name="warn", min_drift_fields=1)
        results = [_make(service="clean", diffs=[])]
        evaluated = evaluate_results(results, [rule])
        assert not evaluated[0].has_breach()
