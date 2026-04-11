"""indexer.py — builds a searchable index of drift results keyed by field name."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from driftwatch.comparator import DriftResult


class IndexerError(Exception):
    """Raised when indexing fails."""


@dataclass
class FieldIndex:
    """Maps each drifted field name to the services that exhibit that drift."""

    index: Dict[str, List[str]] = field(default_factory=dict)

    def field_names(self) -> List[str]:
        """Return sorted list of indexed field names."""
        return sorted(self.index.keys())

    def services_for(self, field_name: str) -> List[str]:
        """Return services that have drift on *field_name*, or empty list."""
        return list(self.index.get(field_name, []))

    def total_fields(self) -> int:
        """Number of distinct drifted fields tracked."""
        return len(self.index)

    def total_entries(self) -> int:
        """Total (field, service) pairs stored."""
        return sum(len(v) for v in self.index.values())

    def summary(self) -> str:
        if not self.index:
            return "index is empty"
        lines = [f"{self.total_fields()} field(s) indexed across {self.total_entries()} service/field pair(s):"]
        for fname in self.field_names():
            services = ", ".join(self.services_for(fname))
            lines.append(f"  {fname}: {services}")
        return "\n".join(lines)


def build_index(results: List[DriftResult]) -> FieldIndex:
    """Build a :class:`FieldIndex` from a list of :class:`DriftResult`.

    Args:
        results: list of DriftResult objects (may be empty).

    Returns:
        FieldIndex populated with every drifted field found.

    Raises:
        IndexerError: if *results* is None.
    """
    if results is None:
        raise IndexerError("results must not be None")

    index: Dict[str, List[str]] = {}
    for result in results:
        for diff in result.diffs:
            fname = diff.field
            if fname not in index:
                index[fname] = []
            if result.service not in index[fname]:
                index[fname].append(result.service)

    # Sort service lists for determinism
    for fname in index:
        index[fname].sort()

    return FieldIndex(index=index)
