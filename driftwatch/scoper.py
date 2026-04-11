"""Scoper: restrict drift results to a declared set of service scopes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class ScoperError(Exception):
    """Raised when scoping configuration or input is invalid."""


@dataclass
class ScopeConfig:
    """Defines which services are in scope for drift evaluation."""

    include: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.include, list):
            raise ScoperError("include must be a list")
        if not isinstance(self.exclude, list):
            raise ScoperError("exclude must be a list")
        overlap = set(self.include) & set(self.exclude)
        if overlap:
            raise ScoperError(
                f"services appear in both include and exclude: {sorted(overlap)}"
            )


@dataclass
class ScopedReport:
    """Results after scoping has been applied."""

    in_scope: List[DriftResult]
    out_of_scope: List[DriftResult]

    @property
    def total_in_scope(self) -> int:
        return len(self.in_scope)

    @property
    def total_out_of_scope(self) -> int:
        return len(self.out_of_scope)

    def summary(self) -> str:
        drift_count = sum(1 for r in self.in_scope if r.has_drift)
        return (
            f"{self.total_in_scope} in scope "
            f"({drift_count} drifted, "
            f"{self.total_in_scope - drift_count} clean), "
            f"{self.total_out_of_scope} out of scope"
        )


def apply_scope(
    results: Optional[List[DriftResult]],
    config: ScopeConfig,
) -> ScopedReport:
    """Partition *results* into in-scope and out-of-scope buckets.

    When *include* is non-empty only listed services are in scope.
    Services listed in *exclude* are always out of scope regardless.
    """
    if results is None:
        raise ScoperError("results must not be None")

    in_scope: List[DriftResult] = []
    out_of_scope: List[DriftResult] = []

    for result in results:
        service = result.service
        if service in config.exclude:
            out_of_scope.append(result)
        elif config.include and service not in config.include:
            out_of_scope.append(result)
        else:
            in_scope.append(result)

    return ScopedReport(in_scope=in_scope, out_of_scope=out_of_scope)
