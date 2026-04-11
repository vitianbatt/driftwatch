"""digester.py – compute deterministic digests (hashes) for DriftResult configs.

Useful for quickly detecting whether a live config has changed between runs
without storing the full payload.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from driftwatch.comparator import DriftResult


class DigesterError(Exception):
    """Raised when digest computation fails."""


@dataclass
class DigestedResult:
    service: str
    digest: str
    drift_fields: List[str] = field(default_factory=list)
    previous_digest: Optional[str] = None

    @property
    def has_changed(self) -> bool:
        """True when a previous digest exists and differs from the current one."""
        return self.previous_digest is not None and self.digest != self.previous_digest

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "digest": self.digest,
            "drift_fields": self.drift_fields,
            "previous_digest": self.previous_digest,
            "has_changed": self.has_changed,
        }


def _stable_json(obj: Any) -> str:
    """Return a deterministic JSON string (sorted keys)."""
    return json.dumps(obj, sort_keys=True, default=str)


def compute_digest(data: Any, algorithm: str = "sha256") -> str:
    """Return a hex digest of *data* serialised to stable JSON."""
    try:
        h = hashlib.new(algorithm)
    except ValueError as exc:
        raise DigesterError(f"Unknown hash algorithm: {algorithm!r}") from exc
    h.update(_stable_json(data).encode())
    return h.hexdigest()


def digest_results(
    results: List[DriftResult],
    previous: Optional[Dict[str, str]] = None,
    algorithm: str = "sha256",
) -> List[DigestedResult]:
    """Compute a digest for each DriftResult.

    Args:
        results:   List of DriftResult objects to digest.
        previous:  Optional mapping of service -> previous digest hex string.
        algorithm: Hash algorithm accepted by :func:`hashlib.new`.

    Returns:
        List of DigestedResult in the same order as *results*.
    """
    if results is None:
        raise DigesterError("results must not be None")
    if previous is None:
        previous = {}

    digested: List[DigestedResult] = []
    for result in results:
        payload = {
            "service": result.service,
            "diffs": [
                {"field": d.field, "expected": d.expected, "actual": d.actual}
                for d in (result.diffs or [])
            ],
        }
        digest = compute_digest(payload, algorithm=algorithm)
        digested.append(
            DigestedResult(
                service=result.service,
                digest=digest,
                drift_fields=[d.field for d in (result.diffs or [])],
                previous_digest=previous.get(result.service),
            )
        )
    return digested
