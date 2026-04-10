"""renamer.py — maps old field names to new canonical names across DriftResults."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class RenamerError(Exception):
    """Raised when rename configuration or input is invalid."""


@dataclass
class RenameMap:
    """Bidirectional alias map: old_name -> canonical_name."""

    mappings: Dict[str, str]

    def __post_init__(self) -> None:
        if self.mappings is None:
            raise RenamerError("mappings must not be None")
        for old, new in self.mappings.items():
            if not old or not old.strip():
                raise RenamerError("mapping key must not be empty or whitespace")
            if not new or not new.strip():
                raise RenamerError("mapping value must not be empty or whitespace")

    def translate(self, name: str) -> str:
        """Return canonical name for *name*, or *name* unchanged if not mapped."""
        return self.mappings.get(name, name)


@dataclass
class RenamedResult:
    """A DriftResult whose field names have been translated via a RenameMap."""

    service: str
    diffs: List[FieldDiff] = field(default_factory=list)
    original: DriftResult = field(repr=False, default=None)  # type: ignore[assignment]

    def has_drift(self) -> bool:
        return bool(self.diffs)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "diffs": [
                {"field": d.field_name, "kind": d.kind, "expected": d.expected, "actual": d.actual}
                for d in self.diffs
            ],
        }


def _rename_diff(diff: FieldDiff, rename_map: RenameMap) -> FieldDiff:
    new_name = rename_map.translate(diff.field_name)
    return FieldDiff(
        field_name=new_name,
        kind=diff.kind,
        expected=diff.expected,
        actual=diff.actual,
    )


def rename_results(
    results: List[DriftResult],
    rename_map: RenameMap,
) -> List[RenamedResult]:
    """Apply *rename_map* to every FieldDiff in *results*."""
    if results is None:
        raise RenamerError("results must not be None")
    if rename_map is None:
        raise RenamerError("rename_map must not be None")

    renamed: List[RenamedResult] = []
    for result in results:
        new_diffs = [_rename_diff(d, rename_map) for d in (result.diffs or [])]
        renamed.append(
            RenamedResult(service=result.service, diffs=new_diffs, original=result)
        )
    return renamed
