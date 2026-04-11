"""Pattern-based service matcher for targeting drift results."""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class MatcherError(Exception):
    """Raised when a matcher rule or operation is invalid."""


@dataclass
class MatchRule:
    pattern: str
    use_regex: bool = False

    def __post_init__(self) -> None:
        if not self.pattern or not self.pattern.strip():
            raise MatcherError("pattern must be a non-empty string")
        if self.use_regex:
            try:
                re.compile(self.pattern)
            except re.error as exc:
                raise MatcherError(f"invalid regex pattern '{self.pattern}': {exc}") from exc

    def matches(self, service: str) -> bool:
        """Return True if *service* matches this rule's pattern."""
        if self.use_regex:
            return bool(re.fullmatch(self.pattern, service))
        return fnmatch.fnmatch(service, self.pattern)


@dataclass
class MatchReport:
    matched: List[DriftResult] = field(default_factory=list)
    unmatched: List[DriftResult] = field(default_factory=list)

    @property
    def total_matched(self) -> int:
        return len(self.matched)

    def summary(self) -> str:
        return (
            f"matched={self.total_matched} unmatched={len(self.unmatched)}"
        )


def match_results(
    results: List[DriftResult],
    rules: List[MatchRule],
    *,
    require_all: bool = False,
) -> MatchReport:
    """Partition *results* into matched / unmatched using *rules*.

    If *require_all* is True a result must satisfy every rule to be matched;
    otherwise any single matching rule is sufficient.
    """
    if results is None:
        raise MatcherError("results must not be None")
    if rules is None:
        raise MatcherError("rules must not be None")

    matched: List[DriftResult] = []
    unmatched: List[DriftResult] = []

    for result in results:
        if require_all:
            hit = rules and all(r.matches(result.service) for r in rules)
        else:
            hit = any(r.matches(result.service) for r in rules)
        (matched if hit else unmatched).append(result)

    return MatchReport(matched=matched, unmatched=unmatched)
