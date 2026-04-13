"""fingerprinter.py — generate stable fingerprints for drift results."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class FingerprinterError(Exception):
    """Raised when fingerprinting fails."""


@dataclass
class FingerprintedResult:
    service: str
    fingerprint: str
    drift_fields: List[str] = field(default_factory=list)
    source_result: Optional[DriftResult] = field(default=None, repr=False)

    def has_drift(self) -> bool:
        return bool(self.drift_fields)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "fingerprint": self.fingerprint,
            "drift_fields": sorted(self.drift_fields),
            "has_drift": self.has_drift(),
        }


def _stable_fingerprint(service: str, diffs: List[FieldDiff]) -> str:
    """Produce a deterministic SHA-256 fingerprint from service name and sorted diffs."""
    payload = {
        "service": service,
        "diffs": sorted(
            [{"field": d.field, "kind": d.kind} for d in diffs],
            key=lambda x: (x["field"], x["kind"]),
        ),
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def fingerprint_results(results: List[DriftResult]) -> List[FingerprintedResult]:
    """Return a fingerprinted result for each DriftResult."""
    if results is None:
        raise FingerprinterError("results must not be None")

    out: List[FingerprintedResult] = []
    for r in results:
        diffs: List[FieldDiff] = getattr(r, "diffs", []) or []
        fp = _stable_fingerprint(r.service, diffs)
        drift_fields = [d.field for d in diffs]
        out.append(
            FingerprintedResult(
                service=r.service,
                fingerprint=fp,
                drift_fields=drift_fields,
                source_result=r,
            )
        )
    return out
