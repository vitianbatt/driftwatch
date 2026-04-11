"""Pinpointer: identify and rank the root fields most responsible for drift."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class PinpointerError(Exception):
    """Raised when pinpointing fails."""


@dataclass
class PinnedField:
    service: str
    field_name: str
    diff_type: str  # "missing", "extra", "changed"
    weight: int

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "field_name": self.field_name,
            "diff_type": self.diff_type,
            "weight": self.weight,
        }


@dataclass
class PinpointReport:
    pinned: List[PinnedField] = field(default_factory=list)

    def top(self, n: int = 5) -> List[PinnedField]:
        return sorted(self.pinned, key=lambda p: p.weight, reverse=True)[:n]

    def summary(self) -> str:
        if not self.pinned:
            return "No drift fields pinpointed."
        lines = [f"Pinpointed {len(self.pinned)} drift field(s):"]
        for p in self.top(5):
            lines.append(f"  [{p.diff_type}] {p.service}.{p.field_name} (weight={p.weight})")
        return "\n".join(lines)


_WEIGHTS = {"missing": 3, "changed": 2, "extra": 1}


def _diff_type(d: FieldDiff) -> str:
    if d.expected is None:
        return "extra"
    if d.actual is None:
        return "missing"
    return "changed"


def pinpoint(results: Optional[List[DriftResult]]) -> PinpointReport:
    """Build a PinpointReport from a list of DriftResult objects."""
    if results is None:
        raise PinpointerError("results must not be None")

    pinned: List[PinnedField] = []
    for result in results:
        for d in result.diffs:
            dtype = _diff_type(d)
            weight = _WEIGHTS.get(dtype, 1)
            pinned.append(
                PinnedField(
                    service=result.service,
                    field_name=d.field,
                    diff_type=dtype,
                    weight=weight,
                )
            )
    return PinpointReport(pinned=pinned)
