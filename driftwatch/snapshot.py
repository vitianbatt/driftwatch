"""Snapshot module: capture and persist point-in-time live config snapshots."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class SnapshotError(Exception):
    """Raised when snapshot operations fail."""


@dataclass
class Snapshot:
    service: str
    timestamp: str
    config: Dict[str, Any]
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "timestamp": self.timestamp,
            "config": self.config,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Snapshot":
        required = {"service", "timestamp", "config"}
        missing = required - data.keys()
        if missing:
            raise SnapshotError(f"Snapshot missing required keys: {missing}")
        return cls(
            service=data["service"],
            timestamp=data["timestamp"],
            config=data["config"],
            tags=data.get("tags", []),
        )


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def take_snapshot(
    service: str,
    config: Dict[str, Any],
    tags: Optional[List[str]] = None,
    timestamp: Optional[str] = None,
) -> Snapshot:
    """Create a Snapshot from a live config dict."""
    return Snapshot(
        service=service,
        timestamp=timestamp or _now_iso(),
        config=config,
        tags=tags or [],
    )


def save_snapshot(snapshot: Snapshot, path: Path) -> None:
    """Append a snapshot as a JSONL line to *path*."""
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(snapshot.to_dict()) + "\n")
    except OSError as exc:
        raise SnapshotError(f"Failed to write snapshot to {path}: {exc}") from exc


def load_snapshots(path: Path, service: Optional[str] = None) -> List[Snapshot]:
    """Load all snapshots from a JSONL file, optionally filtered by service."""
    if not path.exists():
        raise SnapshotError(f"Snapshot file not found: {path}")
    snapshots: List[Snapshot] = []
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                snap = Snapshot.from_dict(data)
            except (json.JSONDecodeError, SnapshotError) as exc:
                raise SnapshotError(f"Invalid snapshot at line {lineno}: {exc}") from exc
            if service is None or snap.service == service:
                snapshots.append(snap)
    return snapshots
