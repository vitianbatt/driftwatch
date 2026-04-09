"""Baseline management: save and load known-good config snapshots."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class BaselineError(Exception):
    """Raised when baseline operations fail."""


@dataclass
class BaselineEntry:
    service: str
    snapshot: Dict[str, Any]
    recorded_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "snapshot": self.snapshot,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaselineEntry":
        return cls(
            service=data["service"],
            snapshot=data["snapshot"],
            recorded_at=data["recorded_at"],
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_baseline(
    path: str | Path,
    service: str,
    snapshot: Dict[str, Any],
    recorded_at: Optional[str] = None,
) -> BaselineEntry:
    """Persist a baseline snapshot for *service* to *path* (JSONL)."""
    path = Path(path)
    entry = BaselineEntry(
        service=service,
        snapshot=snapshot,
        recorded_at=recorded_at or _now_iso(),
    )
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict()) + "\n")
    except OSError as exc:
        raise BaselineError(f"Cannot write baseline to {path}: {exc}") from exc
    return entry


def load_baseline(
    path: str | Path,
    service: str,
) -> Optional[BaselineEntry]:
    """Return the *latest* baseline entry for *service* from *path*, or None."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise BaselineError(f"Cannot read baseline from {path}: {exc}") from exc
    matches = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BaselineError(f"Malformed baseline line in {path}: {exc}") from exc
        if data.get("service") == service:
            matches.append(BaselineEntry.from_dict(data))
    return matches[-1] if matches else None
