"""trimmer.py — Reduce a list of DriftResults to a bounded set.

Provides helpers to cap result lists by count or by a minimum drift-field
threshold, preventing downstream consumers from being overwhelmed when a
large number of services are monitored simultaneously.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class TrimmerError(Exception):
    """Raised when trimmer arguments are invalid."""


@dataclass
class TrimmedReport:
    """Container for trimmed results plus metadata about what was dropped."""

    kept: List[DriftResult]
    dropped_count: int
    original_count: int

    # ------------------------------------------------------------------ #
    # Convenience properties
    # ------------------------------------------------------------------ #

    @property
    def was_trimmed(self) -> bool:
        """Return True if any results were removed."""
        return self.dropped_count > 0

    def summary(self) -> str:
        """Human-readable one-liner describing the trim operation."""
        if not self.was_trimmed:
            return f"All {self.original_count} result(s) kept; nothing trimmed."
        return (
            f"Kept {len(self.kept)} of {self.original_count} result(s); "
            f"{self.dropped_count} dropped."
        )


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #


def _drift_field_count(result: DriftResult) -> int:
    """Return the number of drifted fields in *result*."""
    return len(result.drifted_fields) if result.drifted_fields else 0


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def trim_by_count(
    results: List[DriftResult],
    max_results: int,
    *,
    prefer_drift: bool = True,
) -> TrimmedReport:
    """Keep at most *max_results* entries.

    Parameters
    ----------
    results:
        Source list of :class:`~driftwatch.comparator.DriftResult` objects.
    max_results:
        Maximum number of results to retain.  Must be >= 1.
    prefer_drift:
        When *True* (default) results with drift are sorted to the front
        before the cap is applied, ensuring drifted services are never
        silently dropped in favour of clean ones.

    Returns
    -------
    TrimmedReport
    """
    if results is None:
        raise TrimmerError("results must not be None")
    if max_results < 1:
        raise TrimmerError(f"max_results must be >= 1, got {max_results}")

    original_count = len(results)

    if prefer_drift:
        # Stable sort: drifted first, then by service name for determinism.
        ordered = sorted(
            results,
            key=lambda r: (not r.has_drift, r.service),
        )
    else:
        ordered = list(results)

    kept = ordered[:max_results]
    dropped = original_count - len(kept)
    return TrimmedReport(kept=kept, dropped_count=dropped, original_count=original_count)


def trim_by_threshold(
    results: List[DriftResult],
    min_fields: int,
    *,
    include_clean: bool = False,
) -> TrimmedReport:
    """Keep only results whose drifted-field count meets *min_fields*.

    Parameters
    ----------
    results:
        Source list of :class:`~driftwatch.comparator.DriftResult` objects.
    min_fields:
        Minimum number of drifted fields a result must have to be retained.
        Must be >= 1.
    include_clean:
        When *True*, results with **no** drift are always kept regardless of
        *min_fields*.  Defaults to *False*.

    Returns
    -------
    TrimmedReport
    """
    if results is None:
        raise TrimmerError("results must not be None")
    if min_fields < 1:
        raise TrimmerError(f"min_fields must be >= 1, got {min_fields}")

    original_count = len(results)
    kept: List[DriftResult] = []

    for result in results:
        count = _drift_field_count(result)
        if count == 0 and include_clean:
            kept.append(result)
        elif count >= min_fields:
            kept.append(result)

    dropped = original_count - len(kept)
    return TrimmedReport(kept=kept, dropped_count=dropped, original_count=original_count)
