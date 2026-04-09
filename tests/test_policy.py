"""Tests for driftwatch.policy."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.filter import Severity
from driftwatch.policy import (
    PolicyError,
    PolicyReport,
    PolicyRule,
    evaluate_policy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(service: str, fields: list[str]) -> DriftResult:
    diffs = [FieldDiff(field=f, expected="a", actual="b", kind="changed") for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# PolicyRule validation
# ---------------------------------------------------------------------------

class TestPolicyRule:
    def test_valid_rule_created(self):
        rule = PolicyRule(name="no-high-drift", min_severity=Severity.HIGH, max_violations=0)
        assert rule.name == "no-high-drift"
        assert rule.max_violations == 0

    def test_empty_name_raises(self):
        with pytest.raises(PolicyError, match="name must not be empty"):
            PolicyRule(name="", min_severity=Severity.LOW)

    def test_whitespace_name_raises(self):
        with pytest.raises(PolicyError, match="name must not be empty"):
            PolicyRule(name="   ", min_severity=Severity.LOW)

    def test_negative_max_violations_raises(self):
        with pytest.raises(PolicyError, match="max_violations must be >= 0"):
            PolicyRule(name="r", min_severity=Severity.LOW, max_violations=-1)

    def test_invalid_severity_raises(self):
        with pytest.raises(PolicyError, match="min_severity must be a Severity"):
            PolicyRule(name="r", min_severity="HIGH")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# PolicyReport
# ---------------------------------------------------------------------------

class TestPolicyReport:
    def test_passed_summary(self):
        report = PolicyReport(passed=True)
        assert "PASSED" in report.summary()

    def test_failed_summary_lists_violations(self):
        report = PolicyReport(passed=False, violations=["rule-a: 2 result(s)"])
        assert "FAILED" in report.summary()
        assert "rule-a" in report.summary()


# ---------------------------------------------------------------------------
# evaluate_policy
# ---------------------------------------------------------------------------

class TestEvaluatePolicy:
    def test_none_rules_raises(self):
        with pytest.raises(PolicyError, match="rules must not be None"):
            evaluate_policy(None, [])  # type: ignore[arg-type]

    def test_none_results_raises(self):
        with pytest.raises(PolicyError, match="results must not be None"):
            evaluate_policy([], None)  # type: ignore[arg-type]

    def test_empty_rules_always_passes(self):
        results = [_make("svc", ["key1", "key2", "key3"])]
        report = evaluate_policy([], results)
        assert report.passed is True
        assert report.violations == []

    def test_clean_results_pass_strict_rule(self):
        rule = PolicyRule(name="no-drift", min_severity=Severity.LOW, max_violations=0)
        results = [DriftResult(service="svc", diffs=[])]
        report = evaluate_policy([rule], results)
        assert report.passed is True

    def test_high_drift_violates_zero_tolerance_rule(self):
        rule = PolicyRule(name="no-high", min_severity=Severity.HIGH, max_violations=0)
        # 5 diffs → HIGH severity
        results = [_make("svc", ["a", "b", "c", "d", "e"])]
        report = evaluate_policy([rule], results)
        assert report.passed is False
        assert len(report.violations) == 1

    def test_allows_up_to_max_violations(self):
        rule = PolicyRule(name="allow-one", min_severity=Severity.LOW, max_violations=1)
        results = [_make("svc", ["x"])]
        report = evaluate_policy([rule], results)
        assert report.passed is True

    def test_exceeds_max_violations_fails(self):
        rule = PolicyRule(name="allow-one", min_severity=Severity.LOW, max_violations=1)
        results = [_make("a", ["x"]), _make("b", ["y"])]
        report = evaluate_policy([rule], results)
        assert report.passed is False

    def test_service_scope_filters_correctly(self):
        rule = PolicyRule(name="no-drift", min_severity=Severity.LOW, max_violations=0)
        results = [_make("alpha", ["k"]), _make("beta", ["k"])]
        # scoped to beta only — one violation, exceeds limit
        report = evaluate_policy([rule], results, service="beta")
        assert report.passed is False

    def test_multiple_rules_all_must_pass(self):
        r1 = PolicyRule(name="r1", min_severity=Severity.LOW, max_violations=0)
        r2 = PolicyRule(name="r2", min_severity=Severity.MEDIUM, max_violations=0)
        results = [_make("svc", ["a", "b", "c"])]
        report = evaluate_policy([r1, r2], results)
        assert report.passed is False
        assert len(report.violations) == 2
