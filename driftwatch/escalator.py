"""Escalation engine: promote drift results to escalated alerts based on rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class EscalatorError(Exception):
    """Raised when escalation configuration or processing fails."""


@dataclass
class EscalationRule:
    name: str
    min_severity: Severity
    notify_channel: str
    tags: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise EscalatorError("EscalationRule.name must not be empty")
        if not self.notify_channel or not self.notify_channel.strip():
            raise EscalatorError("EscalationRule.notify_channel must not be empty")
        if not isinstance(self.min_severity, Severity):
            raise EscalatorError(f"min_severity must be a Severity, got {type(self.min_severity)}")


@dataclass
class EscalatedResult:
    result: DriftResult
    rule_name: str
    notify_channel: str
    severity: Severity

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "has_drift": self.result.has_drift,
            "rule_name": self.rule_name,
            "notify_channel": self.notify_channel,
            "severity": self.severity.value,
            "drift_fields": list(self.result.drift_fields),
        }


@dataclass
class EscalationReport:
    escalated: List[EscalatedResult] = field(default_factory=list)
    skipped: List[DriftResult] = field(default_factory=list)

    @property
    def total_escalated(self) -> int:
        return len(self.escalated)

    def summary(self) -> str:
        if not self.escalated:
            return "No results escalated."
        lines = [f"Escalated {self.total_escalated} result(s):"]
        for er in self.escalated:
            lines.append(
                f"  [{er.severity.value.upper()}] {er.result.service} "
                f"-> {er.notify_channel} (rule: {er.rule_name})"
            )
        return "\n".join(lines)


def escalate_results(
    results: List[DriftResult],
    rules: List[EscalationRule],
) -> EscalationReport:
    """Apply escalation rules to a list of DriftResults."""
    if results is None:
        raise EscalatorError("results must not be None")
    if rules is None:
        raise EscalatorError("rules must not be None")

    report = EscalationReport()
    for result in results:
        severity = _result_severity(result)
        matched: Optional[EscalationRule] = None
        for rule in rules:
            if severity.value >= rule.min_severity.value:
                matched = rule
                break
        if matched:
            report.escalated.append(
                EscalatedResult(
                    result=result,
                    rule_name=matched.name,
                    notify_channel=matched.notify_channel,
                    severity=severity,
                )
            )
        else:
            report.skipped.append(result)
    return report
