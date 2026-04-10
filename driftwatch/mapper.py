"""mapper.py — maps drift results to user-defined output field schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from driftwatch.comparator import DriftResult


class MapperError(Exception):
    """Raised when a mapping operation fails."""


@dataclass
class FieldMapping:
    """A single source->destination field name mapping."""
    source: str
    destination: str

    def __post_init__(self) -> None:
        if not self.source or not self.source.strip():
            raise MapperError("source field name must not be empty")
        if not self.destination or not self.destination.strip():
            raise MapperError("destination field name must not be empty")


@dataclass
class MappedResult:
    """A drift result whose output keys have been remapped."""
    service: str
    data: dict[str, Any]
    drift_fields: list[str] = field(default_factory=list)

    def has_drift(self) -> bool:
        return bool(self.drift_fields)

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "data": self.data,
            "drift_fields": self.drift_fields,
        }


def build_mapping(raw: list[dict[str, str]]) -> list[FieldMapping]:
    """Build a list of FieldMapping objects from a raw YAML/JSON list."""
    if raw is None:
        raise MapperError("mapping list must not be None")
    mappings = []
    for item in raw:
        if "source" not in item or "destination" not in item:
            raise MapperError(f"each mapping entry must have 'source' and 'destination': {item}")
        mappings.append(FieldMapping(source=item["source"], destination=item["destination"]))
    return mappings


def apply_mapping(
    results: list[DriftResult],
    mappings: list[FieldMapping],
) -> list[MappedResult]:
    """Apply field mappings to a list of DriftResults.

    Unknown fields are passed through unchanged.
    """
    if results is None:
        raise MapperError("results must not be None")
    if mappings is None:
        raise MapperError("mappings must not be None")

    rename: dict[str, str] = {m.source: m.destination for m in mappings}

    mapped: list[MappedResult] = []
    for result in results:
        raw_data: dict[str, Any] = getattr(result, "diff", {}) or {}
        remapped: dict[str, Any] = {rename.get(k, k): v for k, v in raw_data.items()}
        drift_fields = [rename.get(f, f) for f in (result.drift_fields or [])]
        mapped.append(
            MappedResult(
                service=result.service,
                data=remapped,
                drift_fields=drift_fields,
            )
        )
    return mapped
