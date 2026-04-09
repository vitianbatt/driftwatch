"""Enriches DriftResults with metadata such as environment, region, and owner."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class EnricherError(Exception):
    """Raised when enrichment configuration is invalid."""


@dataclass
class EnrichedResult:
    """A DriftResult decorated with additional metadata."""

    result: DriftResult
    environment: str = ""
    region: str = ""
    owner: str = ""
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "has_drift": self.result.has_drift,
            "missing_keys": self.result.missing_keys,
            "extra_keys": self.result.extra_keys,
            "changed_keys": self.result.changed_keys,
            "environment": self.environment,
            "region": self.region,
            "owner": self.owner,
            "tags": self.tags,
        }


MetaMap = Dict[str, Dict[str, str]]


def _lookup(meta: MetaMap, service: str, key: str) -> str:
    return meta.get(service, {}).get(key, "")


def enrich_results(
    results: List[DriftResult],
    meta: MetaMap,
) -> List[EnrichedResult]:
    """Attach metadata from *meta* to each DriftResult.

    *meta* is a mapping of ``service_name -> {field: value}``.
    Unknown services receive empty strings for all metadata fields.

    Raises:
        EnricherError: if *results* or *meta* is None.
    """
    if results is None:
        raise EnricherError("results must not be None")
    if meta is None:
        raise EnricherError("meta must not be None")

    enriched: List[EnrichedResult] = []
    for result in results:
        svc_meta = meta.get(result.service, {})
        enriched.append(
            EnrichedResult(
                result=result,
                environment=svc_meta.get("environment", ""),
                region=svc_meta.get("region", ""),
                owner=svc_meta.get("owner", ""),
                tags={k: v for k, v in svc_meta.items()
                      if k not in {"environment", "region", "owner"}},
            )
        )
    return enriched
