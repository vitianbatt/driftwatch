"""versioner.py — tracks and compares versioned snapshots of service configs.

Provides VersionedResult and VersionReport to detect when a service's
configuration has changed across two named versions (e.g. 'v1' vs 'v2',
or 'baseline' vs 'current').
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class VersionerError(Exception):
    """Raised when versioning operations fail."""


@dataclass
class VersionedResult:
    """A drift result annotated with version metadata."""

    service: str
    from_version: str
    to_version: str
    diffs: List[FieldDiff] = field(default_factory=list)

    def has_drift(self) -> bool:
        """Return True if any field-level diffs exist between versions."""
        return len(self.diffs) > 0

    def to_dict(self) -> dict:
        """Serialise to a plain dictionary."""
        return {
            "service": self.service,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "has_drift": self.has_drift(),
            "drift_field_count": len(self.diffs),
            "diffs": [
                {
                    "field": d.field,
                    "kind": d.kind,
                    "expected": d.expected,
                    "actual": d.actual,
                }
                for d in self.diffs
            ],
        }


@dataclass
class VersionReport:
    """Aggregated versioning report across multiple services."""

    from_version: str
    to_version: str
    results: List[VersionedResult] = field(default_factory=list)

    def total(self) -> int:
        """Total number of services compared."""
        return len(self.results)

    def drifted(self) -> List[VersionedResult]:
        """Return only results where drift was detected."""
        return [r for r in self.results if r.has_drift()]

    def clean(self) -> List[VersionedResult]:
        """Return only results with no drift."""
        return [r for r in self.results if not r.has_drift()]

    def summary(self) -> str:
        """Human-readable one-line summary."""
        d = len(self.drifted())
        t = self.total()
        return (
            f"{d}/{t} service(s) drifted between "
            f"{self.from_version!r} and {self.to_version!r}"
        )


def build_versioned_result(
    result: DriftResult,
    from_version: str,
    to_version: str,
) -> VersionedResult:
    """Wrap a DriftResult with version labels.

    Args:
        result: A DriftResult produced by the comparator.
        from_version: Label for the baseline / older version.
        to_version: Label for the current / newer version.

    Returns:
        A VersionedResult carrying the same diffs.

    Raises:
        VersionerError: If from_version or to_version are blank.
    """
    if not from_version or not from_version.strip():
        raise VersionerError("from_version must not be empty")
    if not to_version or not to_version.strip():
        raise VersionerError("to_version must not be empty")

    return VersionedResult(
        service=result.service,
        from_version=from_version.strip(),
        to_version=to_version.strip(),
        diffs=list(result.diffs),
    )


def build_version_report(
    results: Optional[List[DriftResult]],
    from_version: str,
    to_version: str,
) -> VersionReport:
    """Build a VersionReport from a list of DriftResults.

    Args:
        results: DriftResults to annotate.
        from_version: Label for the baseline version.
        to_version: Label for the current version.

    Returns:
        A VersionReport containing all annotated results.

    Raises:
        VersionerError: If results is None or versions are blank.
    """
    if results is None:
        raise VersionerError("results must not be None")

    versioned = [
        build_versioned_result(r, from_version, to_version) for r in results
    ]
    return VersionReport(
        from_version=from_version.strip(),
        to_version=to_version.strip(),
        results=versioned,
    )
