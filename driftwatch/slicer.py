"""slicer.py — extract a named subset of fields from drift results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class SlicerError(Exception):
    """Raised when slicing configuration or input is invalid."""


@dataclass
class SliceConfig:
    """Defines which fields to keep in the slice."""

    fields: List[str]

    def __post_init__(self) -> None:
        if self.fields is None:
            raise SlicerError("fields must not be None")
        for f in self.fields:
            if not isinstance(f, str) or not f.strip():
                raise SlicerError("each field name must be a non-empty string")


@dataclass
class SlicedResult:
    """A drift result containing only the requested field diffs."""

    service: str
    kept: List[FieldDiff] = field(default_factory=list)
    omitted_count: int = 0

    def has_drift(self) -> bool:
        return bool(self.kept)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "kept_fields": [d.field for d in self.kept],
            "omitted_count": self.omitted_count,
        }


@dataclass
class SlicedReport:
    """Collection of sliced results."""

    results: List[SlicedResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def summary(self) -> str:
        drifted = sum(1 for r in self.results if r.has_drift())
        return f"{drifted}/{len(self.results)} services have drift in sliced fields"


def slice_results(
    results: Optional[List[DriftResult]],
    config: SliceConfig,
) -> SlicedReport:
    """Return a SlicedReport keeping only diffs for fields listed in config."""
    if results is None:
        raise SlicerError("results must not be None")
    if config is None:
        raise SlicerError("config must not be None")

    allowed = set(config.fields)
    sliced: List[SlicedResult] = []

    for result in results:
        diffs: List[FieldDiff] = result.diffs if result.diffs else []
        kept = [d for d in diffs if d.field in allowed]
        omitted = len(diffs) - len(kept)
        sliced.append(
            SlicedResult(service=result.service, kept=kept, omitted_count=omitted)
        )

    return SlicedReport(results=sliced)
