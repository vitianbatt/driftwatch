"""Dispatcher: route DriftResults to one or more named handlers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List

from driftwatch.comparator import DriftResult


class DispatcherError(Exception):
    """Raised when dispatcher configuration or dispatch fails."""


Handler = Callable[[DriftResult], None]


@dataclass
class DispatchRule:
    """Maps a handler name to a predicate and a handler callable."""

    name: str
    handler: Handler
    predicate: Callable[[DriftResult], bool] = field(default=lambda _: True)

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise DispatcherError("DispatchRule name must be a non-empty string.")
        if not callable(self.handler):
            raise DispatcherError("DispatchRule handler must be callable.")
        if not callable(self.predicate):
            raise DispatcherError("DispatchRule predicate must be callable.")


@dataclass
class DispatchReport:
    """Summary of a dispatch run."""

    dispatched: Dict[str, List[str]] = field(default_factory=dict)  # handler -> [service]
    skipped: List[str] = field(default_factory=list)

    def total_dispatched(self) -> int:
        return sum(len(v) for v in self.dispatched.values())

    def summary(self) -> str:
        if not self.dispatched and not self.skipped:
            return "No results dispatched."
        lines = []
        for handler, services in sorted(self.dispatched.items()):
            lines.append(f"  {handler}: {len(services)} result(s)")
        if self.skipped:
            lines.append(f"  skipped: {len(self.skipped)} result(s)")
        return "Dispatch summary:\n" + "\n".join(lines)


def dispatch(
    results: List[DriftResult],
    rules: List[DispatchRule],
) -> DispatchReport:
    """Route each result through matching rules and return a report."""
    if results is None:
        raise DispatcherError("results must not be None.")
    if rules is None:
        raise DispatcherError("rules must not be None.")

    report = DispatchReport()
    for result in results:
        matched = False
        for rule in rules:
            if rule.predicate(result):
                rule.handler(result)
                report.dispatched.setdefault(rule.name, []).append(result.service)
                matched = True
        if not matched:
            report.skipped.append(result.service)
    return report
