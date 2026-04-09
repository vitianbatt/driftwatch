"""Alerting module: evaluate drift results against thresholds and emit alerts."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class AlertError(Exception):
    """Raised when alert configuration or evaluation fails."""


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    min_severity: Severity
    level: AlertLevel
    message_template: str = "Drift detected in {service}: {summary}"

    def __post_init__(self) -> None:
        if not isinstance(self.min_severity, Severity):
            raise AlertError(f"min_severity must be a Severity, got {type(self.min_severity)}")
        if not isinstance(self.level, AlertLevel):
            raise AlertError(f"level must be an AlertLevel, got {type(self.level)}")


@dataclass
class Alert:
    service: str
    level: AlertLevel
    message: str
    result: DriftResult


@dataclass
class AlertConfig:
    rules: List[AlertRule] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.rules:
            raise AlertError("AlertConfig requires at least one rule")


def evaluate_alerts(
    results: List[DriftResult],
    config: AlertConfig,
) -> List[Alert]:
    """Evaluate drift results against alert rules and return triggered alerts."""
    if not isinstance(config, AlertConfig):
        raise AlertError("config must be an AlertConfig instance")

    alerts: List[Alert] = []
    for result in results:
        severity = _result_severity(result)
        for rule in config.rules:
            if severity.value >= rule.min_severity.value:
                from driftwatch.comparator import summary as drift_summary
                msg = rule.message_template.format(
                    service=result.service,
                    summary=drift_summary(result),
                )
                alerts.append(Alert(
                    service=result.service,
                    level=rule.level,
                    message=msg,
                    result=result,
                ))
                break  # first matching rule wins
    return alerts
