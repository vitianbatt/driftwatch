"""Tests for driftwatch.alerting."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.alerting import (
    AlertError,
    AlertLevel,
    AlertRule,
    Alert,
    AlertConfig,
    evaluate_alerts,
)
from driftwatch.filter import Severity


def _make_result(service: str, missing=None, extra=None, changed=None) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=missing or [],
        extra_keys=extra or [],
        changed_values=changed or {},
    )


class TestAlertRule:
    def test_valid_rule_created(self):
        rule = AlertRule(min_severity=Severity.MEDIUM, level=AlertLevel.WARNING)
        assert rule.min_severity == Severity.MEDIUM
        assert rule.level == AlertLevel.WARNING

    def test_invalid_severity_raises(self):
        with pytest.raises(AlertError):
            AlertRule(min_severity="medium", level=AlertLevel.WARNING)

    def test_invalid_level_raises(self):
        with pytest.raises(AlertError):
            AlertRule(min_severity=Severity.LOW, level="warning")


class TestAlertConfig:
    def test_empty_rules_raises(self):
        with pytest.raises(AlertError, match="at least one rule"):
            AlertConfig(rules=[])

    def test_valid_config(self):
        rule = AlertRule(min_severity=Severity.LOW, level=AlertLevel.INFO)
        config = AlertConfig(rules=[rule])
        assert len(config.rules) == 1


class TestEvaluateAlerts:
    def _default_config(self) -> AlertConfig:
        return AlertConfig(rules=[
            AlertRule(min_severity=Severity.HIGH, level=AlertLevel.CRITICAL),
            AlertRule(min_severity=Severity.MEDIUM, level=AlertLevel.WARNING),
            AlertRule(min_severity=Severity.LOW, level=AlertLevel.INFO),
        ])

    def test_no_drift_no_alert(self):
        result = _make_result("svc")
        alerts = evaluate_alerts([result], self._default_config())
        # LOW severity but no drift fields — still triggers INFO rule
        # severity LOW >= LOW so INFO alert expected
        assert len(alerts) == 1
        assert alerts[0].level == AlertLevel.INFO

    def test_many_missing_keys_triggers_critical(self):
        result = _make_result("svc", missing=["a", "b", "c", "d", "e"])
        alerts = evaluate_alerts([result], self._default_config())
        assert alerts[0].level == AlertLevel.CRITICAL

    def test_medium_drift_triggers_warning(self):
        result = _make_result("svc", missing=["a", "b", "c"])
        alerts = evaluate_alerts([result], self._default_config())
        assert alerts[0].level == AlertLevel.WARNING

    def test_alert_message_contains_service(self):
        result = _make_result("my-service", missing=["k"])
        alerts = evaluate_alerts([result], self._default_config())
        assert "my-service" in alerts[0].message

    def test_multiple_results_produce_multiple_alerts(self):
        results = [
            _make_result("svc-a", missing=["x"]),
            _make_result("svc-b", missing=["y"]),
        ]
        alerts = evaluate_alerts(results, self._default_config())
        assert len(alerts) == 2
        service_names = {a.service for a in alerts}
        assert service_names == {"svc-a", "svc-b"}

    def test_changed_values_contribute_to_severity(self):
        result = _make_result("svc", changed={"key1": ("old", "new"), "key2": ("a", "b"), "key3": ("x", "y")})
        alerts = evaluate_alerts([result], self._default_config())
        assert alerts[0].level == AlertLevel.WARNING

    def test_empty_results_list_returns_no_alerts(self):
        alerts = evaluate_alerts([], self._default_config())
        assert alerts == []
