"""heatmap.py — builds a drift heatmap showing how often each field drifts per service."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class HeatmapError(Exception):
    """Raised when heatmap construction fails."""


@dataclass
class HeatCell:
    """Represents the drift frequency for a single (service, field) pair."""

    service: str
    field_name: str
    count: int

    def to_dict(self) -> dict:
        return {"service": self.service, "field": self.field_name, "count": self.count}


@dataclass
class HeatmapReport:
    """Aggregated heatmap across all results."""

    cells: List[HeatCell] = field(default_factory=list)

    def hottest(self, n: int = 5) -> List[HeatCell]:
        """Return the top-n cells by drift count."""
        return sorted(self.cells, key=lambda c: c.count, reverse=True)[:n]

    def services(self) -> List[str]:
        """Return sorted unique service names present in the heatmap."""
        return sorted({c.service for c in self.cells})

    def fields(self) -> List[str]:
        """Return sorted unique field names present in the heatmap."""
        return sorted({c.field_name for c in self.cells})

    def get(self, service: str, field_name: str) -> int:
        """Return the drift count for a specific (service, field) pair, or 0."""
        for cell in self.cells:
            if cell.service == service and cell.field_name == field_name:
                return cell.count
        return 0

    def summary(self) -> str:
        if not self.cells:
            return "Heatmap: no drift data."
        top = self.hottest(3)
        lines = ["Drift heatmap (top entries):"]
        for cell in top:
            lines.append(f"  {cell.service}/{cell.field_name}: {cell.count}")
        return "\n".join(lines)


def build_heatmap(results: Optional[List[DriftResult]]) -> HeatmapReport:
    """Build a HeatmapReport from a list of DriftResult objects."""
    if results is None:
        raise HeatmapError("results must not be None")

    counts: Dict[tuple, int] = {}
    for result in results:
        if not hasattr(result, "diffs") or result.diffs is None:
            continue
        for diff in result.diffs:
            key = (result.service, diff.field)
            counts[key] = counts.get(key, 0) + 1

    cells = [
        HeatCell(service=svc, field_name=fld, count=cnt)
        for (svc, fld), cnt in counts.items()
    ]
    cells.sort(key=lambda c: (c.service, c.field_name))
    return HeatmapReport(cells=cells)
