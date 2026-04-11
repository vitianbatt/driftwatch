"""Tests for driftwatch.escalator."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity
from driftwatch.escalator import (
    EscalatorError,
    EscalationRule,
    EscalatedResult,
    EscalationReport,
    escalate_results,
)


def _make(service: str, drift_fields=None) -> DriftResult:
    return DriftResult(
        service=service,
        has_drift=bool(drift_fields),
        drift_fields=set(drift_fields or []),
    )


# ---------------------------------------------------------------------------
# TestEscalationRule
# ---------------------------------------------------------------------------
class TestEscalationRule:
    def test_valid_rule_created(self):
        rule = EscalationRule(name="crit", min_severity=Severity.HIGH, notify_channel="pagerduty")
        assert rule.name == "crit"
        assert rule.notify_channel == "pagerduty"

    def test_empty_name_raises(self):
        with pytest.raises(EscalatorError, match="name"):
            EscalationRule(name="", min_severity=Severity.LOW, notify_channel="slack")

    def test_whitespace_name_raises(self):
        with pytest.raises(EscalatorError, match="name"):
            EscalationRule(name="   ", min_severity=Severity.LOW, notify_channel="slack")

    def test_empty_channel_raises(self):
        with pytest.raises(EscalatorError, match="notify_channel"):
            EscalationRule(name="r", min_severity=Severity.LOW, notify_channel="")

    def test_invalid_severity_type_raises(self):
        with pytest.raises(EscalatorError, match="min_severity"):
            EscalationRule(name="r", min_severity="high", notify_channel="slack")  # type: ignore


# ---------------------------------------------------------------------------
# TestEscalateResults
# ---------------------------------------------------------------------------
class TestEscalateResults:
    def _rule(self, name, severity, channel="slack"):
        return EscalationRule(name=name, min_severity=severity, notify_channel=channel)

    def test_none_results_raises(self):
        with pytest.raises(EscalatorError):
            escalate_results(None, [])  # type: ignore

    def test_none_rules_raises(self):
        with pytest.raises(EscalatorError):
            escalate_results([], None)  # type: ignore

    def test_empty_results_returns_empty_report(self):
        report = escalate_results([], [])
        assert report.total_escalated == 0
        assert report.skipped == []

    def test_clean_result_with_no_matching_rule_is_skipped(self):
        result = _make("svc-a")
        rule = self._rule("high-only", Severity.HIGH)
        report = escalate_results([result], [rule])
        assert report.total_escalated == 0
        assert len(report.skipped) == 1

    def test_drifted_result_matches_low_rule(self):
        result = _make("svc-b", ["replicas"])
        rule = self._rule("any-drift", Severity.LOW, "email")
        report = escalate_results([result], [rule])
        assert report.total_escalated == 1
        assert report.escalated[0].notify_channel == "email"

    def test_escalated_result_to_dict(self):
        result = _make("svc-c", ["image", "env"])
        rule = self._rule("medium-plus", Severity.MEDIUM, "pagerduty")
        report = escalate_results([result], [rule])
        d = report.escalated[0].to_dict()
        assert d["service"] == "svc-c"
        assert d["notify_channel"] == "pagerduty"
        assert d["has_drift"] is True

    def test_first_matching_rule_wins(self):
        result = _make("svc-d", ["x", "y", "z", "w"])
        rules = [
            self._rule("high-rule", Severity.HIGH, "pagerduty"),
            self._rule("low-rule", Severity.LOW, "slack"),
        ]
        report = escalate_results([result], rules)
        assert report.escalated[0].notify_channel == "pagerduty"

    def test_summary_no_escalations(self):
        report = EscalationReport()
        assert "No results escalated" in report.summary()

    def test_summary_with_escalations(self):
        result = _make("svc-e", ["cpu"])
        rule = self._rule("low-rule", Severity.LOW, "slack")
        report = escalate_results([result], [rule])
        s = report.summary()
        assert "svc-e" in s
        assert "slack" in s
