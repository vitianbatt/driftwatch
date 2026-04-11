"""Route drift results to named destinations based on service patterns."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class RouterError(Exception):
    """Raised when routing configuration or execution fails."""


@dataclass
class RouteRule:
    """Maps a service name pattern to a named destination."""

    destination: str
    pattern: str = "*"
    _regex: re.Pattern = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.destination or not self.destination.strip():
            raise RouterError("destination must be a non-empty string")
        if not self.pattern or not self.pattern.strip():
            raise RouterError("pattern must be a non-empty string")
        glob = re.escape(self.pattern).replace(r"\*", ".*")
        try:
            self._regex = re.compile(f"^{glob}$")
        except re.error as exc:
            raise RouterError(f"invalid pattern '{self.pattern}': {exc}") from exc

    def matches(self, service: str) -> bool:
        return bool(self._regex.match(service))


@dataclass
class RoutedReport:
    """Holds drift results partitioned by destination."""

    routes: Dict[str, List[DriftResult]] = field(default_factory=dict)
    unrouted: List[DriftResult] = field(default_factory=list)

    def destination_names(self) -> List[str]:
        return sorted(self.routes.keys())

    def size(self, destination: str) -> int:
        return len(self.routes.get(destination, []))

    def total(self) -> int:
        return sum(len(v) for v in self.routes.values()) + len(self.unrouted)

    def summary(self) -> str:
        parts = [f"{d}:{self.size(d)}" for d in self.destination_names()]
        if self.unrouted:
            parts.append(f"unrouted:{len(self.unrouted)}")
        return ", ".join(parts) if parts else "no results"


def route_results(
    results: List[DriftResult],
    rules: List[RouteRule],
    *,
    allow_unrouted: bool = True,
) -> RoutedReport:
    """Assign each result to the first matching rule's destination.

    Args:
        results: Drift results to route.
        rules: Ordered list of routing rules.
        allow_unrouted: When False, raises RouterError for unmatched results.

    Returns:
        RoutedReport with results partitioned by destination.
    """
    if results is None:
        raise RouterError("results must not be None")
    if rules is None:
        raise RouterError("rules must not be None")

    report = RoutedReport()
    for result in results:
        matched: Optional[str] = None
        for rule in rules:
            if rule.matches(result.service):
                matched = rule.destination
                break
        if matched is not None:
            report.routes.setdefault(matched, []).append(result)
        elif allow_unrouted:
            report.unrouted.append(result)
        else:
            raise RouterError(
                f"no route matched service '{result.service}' and allow_unrouted=False"
            )
    return report
