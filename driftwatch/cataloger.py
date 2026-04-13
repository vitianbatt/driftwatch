"""cataloger.py — builds a searchable catalog of drift results keyed by service name."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class CatalogerError(Exception):
    """Raised when catalog operations fail."""


@dataclass
class CatalogEntry:
    service: str
    results: List[DriftResult] = field(default_factory=list)

    @property
    def drift_count(self) -> int:
        return sum(1 for r in self.results if r.drifted_fields)

    @property
    def has_drift(self) -> bool:
        return self.drift_count > 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "total_results": len(self.results),
            "drift_count": self.drift_count,
            "has_drift": self.has_drift,
        }


@dataclass
class CatalogReport:
    entries: Dict[str, CatalogEntry] = field(default_factory=dict)

    def service_names(self) -> List[str]:
        return sorted(self.entries.keys())

    def get(self, service: str) -> Optional[CatalogEntry]:
        return self.entries.get(service)

    def total_services(self) -> int:
        return len(self.entries)

    def drifted_services(self) -> List[str]:
        return sorted(s for s, e in self.entries.items() if e.has_drift)

    def summary(self) -> str:
        total = self.total_services()
        drifted = len(self.drifted_services())
        return f"{drifted}/{total} services have drift"


def build_catalog(results: List[DriftResult]) -> CatalogReport:
    """Group DriftResult objects by service into a CatalogReport."""
    if results is None:
        raise CatalogerError("results must not be None")

    catalog: Dict[str, CatalogEntry] = {}
    for result in results:
        service = result.service
        if not service or not service.strip():
            raise CatalogerError("DriftResult has empty service name")
        if service not in catalog:
            catalog[service] = CatalogEntry(service=service)
        catalog[service].results.append(result)

    return CatalogReport(entries=catalog)
