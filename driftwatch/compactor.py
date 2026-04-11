"""Compactor: reduces a list of DriftResults by merging repeated service entries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class CompactorError(Exception):
    """Raised when compaction fails."""


@dataclass
class CompactedResult:
    service: str
    drift_fields: List[FieldDiff] = field(default_factory=list)
    source_count: int = 1

    @property
    def has_drift(self) -> bool:
        return bool(self.drift_fields)

    def summary(self) -> str:
        if not self.has_drift:
            return f"{self.service}: clean (merged {self.source_count} entries)"
        names = ", ".join(d.field for d in self.drift_fields)
        return (
            f"{self.service}: {len(self.drift_fields)} drift field(s) "
            f"[{names}] (merged {self.source_count} entries)"
        )

    def to_dict(self) -> Dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift,
            "drift_fields": [d.field for d in self.drift_fields],
            "source_count": self.source_count,
        }


def compact_results(results: List[DriftResult]) -> List[CompactedResult]:
    """Merge multiple DriftResult entries for the same service.

    Drift fields are unioned across all entries for a given service.
    Raises CompactorError if results is None.
    """
    if results is None:
        raise CompactorError("results must not be None")

    merged: Dict[str, CompactedResult] = {}
    for r in results:
        if r.service in merged:
            existing = merged[r.service]
            seen_fields = {d.field for d in existing.drift_fields}
            for d in r.drift_fields:
                if d.field not in seen_fields:
                    existing.drift_fields.append(d)
                    seen_fields.add(d.field)
            existing.source_count += 1
        else:
            merged[r.service] = CompactedResult(
                service=r.service,
                drift_fields=list(r.drift_fields),
                source_count=1,
            )
    return list(merged.values())
