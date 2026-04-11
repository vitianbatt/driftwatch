"""stenciler.py – apply a field-inclusion stencil to drift results.

A StencilConfig defines which fields are "in scope" for reporting.
Any drift fields not listed in the stencil are stripped before output.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class StencilerError(Exception):
    """Raised when stencil configuration or application fails."""


@dataclass
class StencilConfig:
    """Defines the set of field names that should be retained."""

    allowed_fields: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.allowed_fields is None:
            raise StencilerError("allowed_fields must not be None")
        cleaned = [f.strip() for f in self.allowed_fields]
        if any(f == "" for f in cleaned):
            raise StencilerError("allowed_fields must not contain blank entries")
        self.allowed_fields = cleaned

    def allows(self, field_name: str) -> bool:
        """Return True when the field is permitted by this stencil.

        An empty allowed_fields list means *all* fields are permitted.
        """
        if not self.allowed_fields:
            return True
        return field_name in self.allowed_fields


@dataclass
class StenciledResult:
    """A DriftResult with drift fields filtered through a stencil."""

    service: str
    original_diff_count: int
    diffs: List[FieldDiff] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.diffs)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "original_diff_count": self.original_diff_count,
            "retained_diff_count": len(self.diffs),
            "has_drift": self.has_drift,
            "diffs": [str(d) for d in self.diffs],
        }


def apply_stencil(
    results: List[DriftResult],
    config: StencilConfig,
) -> List[StenciledResult]:
    """Filter each result's diffs to only those allowed by *config*.

    Parameters
    ----------
    results:
        Raw drift results from the comparator.
    config:
        Stencil specifying which fields to retain.

    Returns
    -------
    List[StenciledResult]
        One entry per input result, with diffs pruned to the stencil.
    """
    if results is None:
        raise StencilerError("results must not be None")
    if config is None:
        raise StencilerError("config must not be None")

    out: List[StenciledResult] = []
    for r in results:
        kept = [d for d in (r.diffs or []) if config.allows(d.field)]
        out.append(
            StenciledResult(
                service=r.service,
                original_diff_count=len(r.diffs or []),
                diffs=kept,
            )
        )
    return out
