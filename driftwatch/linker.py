"""linker.py – links DriftResults to related services via a dependency map."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class LinkerError(Exception):
    """Raised when linking fails."""


@dataclass
class DependencyMap:
    """Maps a service name to the list of services it depends on."""

    deps: Dict[str, List[str]]

    def __post_init__(self) -> None:
        if self.deps is None:
            raise LinkerError("deps must not be None")
        for svc, dependents in self.deps.items():
            if not svc or not svc.strip():
                raise LinkerError("service key must not be empty or whitespace")
            if not isinstance(dependents, list):
                raise LinkerError(f"deps for '{svc}' must be a list")

    def dependencies_of(self, service: str) -> List[str]:
        """Return the direct dependencies of *service* (empty list if unknown)."""
        return list(self.deps.get(service, []))


@dataclass
class LinkedResult:
    """A DriftResult enriched with dependency information."""

    result: DriftResult
    dependencies: List[str] = field(default_factory=list)
    affected_by: List[str] = field(default_factory=list)

    def has_upstream_drift(self, drifted_services: List[str]) -> bool:
        """Return True if any dependency is in *drifted_services*."""
        return any(dep in drifted_services for dep in self.dependencies)

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "has_drift": self.result.has_drift(),
            "dependencies": self.dependencies,
            "affected_by": self.affected_by,
        }


def link_results(
    results: Optional[List[DriftResult]],
    dep_map: Optional[DependencyMap],
) -> List[LinkedResult]:
    """Attach dependency metadata to each result."""
    if results is None:
        raise LinkerError("results must not be None")
    if dep_map is None:
        raise LinkerError("dep_map must not be None")

    drifted = {r.service for r in results if r.has_drift()}

    linked: List[LinkedResult] = []
    for result in results:
        deps = dep_map.dependencies_of(result.service)
        affected_by = [d for d in deps if d in drifted]
        linked.append(LinkedResult(result=result, dependencies=deps, affected_by=affected_by))
    return linked
