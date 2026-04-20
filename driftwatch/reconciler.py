"""reconciler.py — Reconciles drift results against a known-good baseline.

Provides logic to determine which drifted fields are 'new' (not previously
seen in baseline), 'resolved' (were drifted before, now clean), or
'persistent' (still drifting from a prior run).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class ReconcilerError(Exception):
    """Raised when reconciliation fails due to invalid input."""


@dataclass
class ReconciledResult:
    """Holds the reconciled state of a single service's drift."""

    service: str
    new_fields: List[str] = field(default_factory=list)
    resolved_fields: List[str] = field(default_factory=list)
    persistent_fields: List[str] = field(default_factory=list)
    # All current diffs, regardless of reconciliation state
    current_diffs: List[FieldDiff] = field(default_factory=list)

    @property
    def has_new_drift(self) -> bool:
        """True if any fields are newly drifted since the baseline."""
        return bool(self.new_fields)

    @property
    def has_resolved(self) -> bool:
        """True if any previously drifted fields are now clean."""
        return bool(self.resolved_fields)

    @property
    def is_clean(self) -> bool:
        """True if there is no current drift at all."""
        return not self.current_diffs

    def to_dict(self) -> Dict:
        return {
            "service": self.service,
            "new_fields": sorted(self.new_fields),
            "resolved_fields": sorted(self.resolved_fields),
            "persistent_fields": sorted(self.persistent_fields),
            "has_new_drift": self.has_new_drift,
            "has_resolved": self.has_resolved,
            "is_clean": self.is_clean,
        }


@dataclass
class ReconciliationReport:
    """Aggregated reconciliation results across all services."""

    results: List[ReconciledResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def newly_drifted_count(self) -> int:
        """Number of services with at least one new drifted field."""
        return sum(1 for r in self.results if r.has_new_drift)

    @property
    def resolved_count(self) -> int:
        """Number of services where at least one field was resolved."""
        return sum(1 for r in self.results if r.has_resolved)

    def summary(self) -> str:
        lines = [
            f"Reconciliation report: {self.total} service(s) evaluated.",
            f"  Newly drifted: {self.newly_drifted_count}",
            f"  Resolved:      {self.resolved_count}",
        ]
        for r in self.results:
            if r.has_new_drift or r.has_resolved:
                lines.append(f"  [{r.service}]")
                if r.new_fields:
                    lines.append(f"    new:        {', '.join(sorted(r.new_fields))}")
                if r.resolved_fields:
                    lines.append(f"    resolved:   {', '.join(sorted(r.resolved_fields))}")
                if r.persistent_fields:
                    lines.append(f"    persistent: {', '.join(sorted(r.persistent_fields))}")
        return "\n".join(lines)


def reconcile(
    current_results: List[DriftResult],
    baseline_drifted: Optional[Dict[str, Set[str]]] = None,
) -> ReconciliationReport:
    """Reconcile current drift results against a baseline.

    Args:
        current_results: Fresh DriftResult objects from the latest watch run.
        baseline_drifted: Mapping of service name -> set of field names that
            were drifted in the baseline snapshot.  Pass None or an empty dict
            to treat everything as new.

    Returns:
        A ReconciliationReport describing new, resolved, and persistent drift.

    Raises:
        ReconcilerError: If current_results is None.
    """
    if current_results is None:
        raise ReconcilerError("current_results must not be None")

    baseline: Dict[str, Set[str]] = baseline_drifted or {}
    reconciled: List[ReconciledResult] = []

    for result in current_results:
        service = result.service
        current_fields: Set[str] = {
            d.field for d in (result.diffs or [])
        }
        prior_fields: Set[str] = baseline.get(service, set())

        new_fields = sorted(current_fields - prior_fields)
        resolved_fields = sorted(prior_fields - current_fields)
        persistent_fields = sorted(current_fields & prior_fields)

        reconciled.append(
            ReconciledResult(
                service=service,
                new_fields=new_fields,
                resolved_fields=resolved_fields,
                persistent_fields=persistent_fields,
                current_diffs=list(result.diffs or []),
            )
        )

    return ReconciliationReport(results=reconciled)
