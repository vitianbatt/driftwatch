"""Policy enforcement for drift results — define pass/fail rules based on drift thresholds."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class PolicyError(Exception):
    """Raised when a policy is misconfigured or evaluation fails."""


@dataclass
class PolicyRule:
    """A single policy rule that fails when too many results exceed a severity threshold."""

    name: str
    min_severity: Severity
    max_violations: int = 0

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise PolicyError("Policy rule name must not be empty.")
        if self.max_violations < 0:
            raise PolicyError("max_violations must be >= 0.")
        if not isinstance(self.min_severity, Severity):
            raise PolicyError(f"min_severity must be a Severity instance, got {self.min_severity!r}.")


@dataclass
class PolicyReport:
    """Outcome of evaluating a set of policy rules against drift results."""

    passed: bool
    violations: List[str] = field(default_factory=list)

    def summary(self) -> str:
        if self.passed:
            return "Policy check PASSED — no rules violated."
        joined = "; ".join(self.violations)
        return f"Policy check FAILED — {len(self.violations)} rule(s) violated: {joined}"


def evaluate_policy(
    rules: List[PolicyRule],
    results: List[DriftResult],
    service: Optional[str] = None,
) -> PolicyReport:
    """Evaluate *rules* against *results*, optionally scoped to a single service.

    Returns a :class:`PolicyReport` describing which rules (if any) were violated.
    """
    if rules is None:
        raise PolicyError("rules must not be None.")
    if results is None:
        raise PolicyError("results must not be None.")

    scoped = (
        [r for r in results if r.service == service] if service is not None else list(results)
    )

    violations: List[str] = []

    for rule in rules:
        count = sum(
            1
            for r in scoped
            if _result_severity(r).value >= rule.min_severity.value and r.diffs
        )
        if count > rule.max_violations:
            violations.append(
                f"'{rule.name}': {count} result(s) at or above {rule.min_severity.name} "
                f"(limit {rule.max_violations})"
            )

    return PolicyReport(passed=len(violations) == 0, violations=violations)
