"""zipper.py – pairs spec fields with live config fields for side-by-side comparison."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from driftwatch.comparator import DriftResult


class ZipperError(Exception):
    """Raised when zipping fails."""


@dataclass
class ZippedField:
    field_name: str
    spec_value: Any
    live_value: Any
    is_missing: bool = False   # present in spec, absent in live
    is_extra: bool = False     # present in live, absent in spec

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field": self.field_name,
            "spec": self.spec_value,
            "live": self.live_value,
            "is_missing": self.is_missing,
            "is_extra": self.is_extra,
        }


@dataclass
class ZippedResult:
    service: str
    fields: List[ZippedField] = field(default_factory=list)

    def has_drift(self) -> bool:
        return any(f.spec_value != f.live_value for f in self.fields)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "fields": [f.to_dict() for f in self.fields],
        }


def zip_result(result: DriftResult, spec: Dict[str, Any], live: Dict[str, Any]) -> ZippedResult:
    """Produce a ZippedResult that pairs every spec key with its live counterpart."""
    if result is None:
        raise ZipperError("result must not be None")
    if spec is None:
        raise ZipperError("spec must not be None")
    if live is None:
        raise ZipperError("live must not be None")

    all_keys = sorted(set(spec) | set(live))
    zipped_fields: List[ZippedField] = []
    for key in all_keys:
        in_spec = key in spec
        in_live = key in live
        zipped_fields.append(
            ZippedField(
                field_name=key,
                spec_value=spec.get(key),
                live_value=live.get(key),
                is_missing=in_spec and not in_live,
                is_extra=in_live and not in_spec,
            )
        )
    return ZippedResult(service=result.service, fields=zipped_fields)


def zip_all(
    results: Optional[List[DriftResult]],
    specs: Dict[str, Dict[str, Any]],
    lives: Dict[str, Dict[str, Any]],
) -> List[ZippedResult]:
    """Zip every result in *results* using matching entries from *specs* and *lives*."""
    if results is None:
        raise ZipperError("results must not be None")
    return [
        zip_result(r, specs.get(r.service, {}), lives.get(r.service, {}))
        for r in results
    ]
