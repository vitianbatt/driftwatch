"""Audit log for drift detection events."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from driftwatch.comparator import DriftResult

logger = logging.getLogger(__name__)


class AuditError(Exception):
    """Raised when an audit operation fails."""


@dataclass
class AuditEntry:
    service: str
    timestamp: str
    has_drift: bool
    drift_count: int
    fields: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "timestamp": self.timestamp,
            "has_drift": self.has_drift,
            "drift_count": self.drift_count,
            "fields": self.fields,
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_entry(result: DriftResult, timestamp: Optional[str] = None) -> AuditEntry:
    """Build an AuditEntry from a DriftResult."""
    ts = timestamp or _now_iso()
    drifted_fields = list(result.diffs.keys()) if result.diffs else []
    return AuditEntry(
        service=result.service,
        timestamp=ts,
        has_drift=result.has_drift,
        drift_count=len(drifted_fields),
        fields=drifted_fields,
    )


def append_audit_log(entry: AuditEntry, log_path: Path) -> None:
    """Append a single audit entry as a JSON line to *log_path*."""
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict()) + "\n")
    except OSError as exc:
        raise AuditError(f"Failed to write audit log '{log_path}': {exc}") from exc


def read_audit_log(log_path: Path) -> List[AuditEntry]:
    """Read all audit entries from *log_path*."""
    if not log_path.exists():
        return []
    entries: List[AuditEntry] = []
    try:
        with log_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                raw = json.loads(line)
                entries.append(
                    AuditEntry(
                        service=raw["service"],
                        timestamp=raw["timestamp"],
                        has_drift=raw["has_drift"],
                        drift_count=raw["drift_count"],
                        fields=raw.get("fields", []),
                    )
                )
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        raise AuditError(f"Failed to read audit log '{log_path}': {exc}") from exc
    return entries
