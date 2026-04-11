"""Resolver: maps drift results to owner/team metadata for accountability tracking."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class ResolverError(Exception):
    """Raised when resolution fails."""


@dataclass
class OwnerMap:
    """Maps service names to owner/team strings."""

    mappings: Dict[str, str]

    def __post_init__(self) -> None:
        if not isinstance(self.mappings, dict):
            raise ResolverError("mappings must be a dict")
        for svc, owner in self.mappings.items():
            if not svc or not svc.strip():
                raise ResolverError("service key must not be blank")
            if not owner or not owner.strip():
                raise ResolverError(f"owner for '{svc}' must not be blank")

    def lookup(self, service: str) -> Optional[str]:
        """Return owner for *service*, or None if not mapped."""
        return self.mappings.get(service)


@dataclass
class ResolvedResult:
    """A DriftResult decorated with an owner string."""

    result: DriftResult
    owner: Optional[str] = None

    def has_owner(self) -> bool:
        return self.owner is not None

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "owner": self.owner,
            "has_drift": self.result.has_drift(),
            "drift_fields": list(self.result.drift_fields),
        }


def resolve_results(
    results: List[DriftResult],
    owner_map: OwnerMap,
) -> List[ResolvedResult]:
    """Attach owner metadata to each DriftResult."""
    if results is None:
        raise ResolverError("results must not be None")
    if owner_map is None:
        raise ResolverError("owner_map must not be None")
    return [
        ResolvedResult(result=r, owner=owner_map.lookup(r.service))
        for r in results
    ]


def unowned(resolved: List[ResolvedResult]) -> List[ResolvedResult]:
    """Return only results with no owner assigned."""
    return [r for r in resolved if not r.has_owner()]
