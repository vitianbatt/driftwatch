"""archiver.py — persist and retrieve drift result archives by date bucket."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

from driftwatch.comparator import DriftResult


class ArchiverError(Exception):
    """Raised when archiving operations fail."""


@dataclass
class ArchiveEntry:
    service: str
    archived_at: str
    drifted: bool
    drift_fields: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "archived_at": self.archived_at,
            "drifted": self.drifted,
            "drift_fields": self.drift_fields,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveEntry":
        required = {"service", "archived_at", "drifted"}
        missing = required - data.keys()
        if missing:
            raise ArchiverError(f"ArchiveEntry missing keys: {missing}")
        return cls(
            service=data["service"],
            archived_at=data["archived_at"],
            drifted=data["drifted"],
            drift_fields=data.get("drift_fields", []),
        )


def _today_iso() -> str:
    return date.today().isoformat()


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def archive_results(
    results: List[DriftResult],
    archive_dir: Path,
    bucket: Optional[str] = None,
) -> Path:
    """Write results to a date-bucketed .jsonl file under archive_dir."""
    if results is None:
        raise ArchiverError("results must not be None")
    if not isinstance(archive_dir, Path):
        archive_dir = Path(archive_dir)
    bucket = bucket or _today_iso()
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / f"{bucket}.jsonl"
    now = _now_iso()
    with target.open("a", encoding="utf-8") as fh:
        for r in results:
            entry = ArchiveEntry(
                service=r.service,
                archived_at=now,
                drifted=r.has_drift,
                drift_fields=list(r.missing_keys | r.extra_keys | r.changed_keys),
            )
            fh.write(json.dumps(entry.to_dict()) + "\n")
    return target


def load_archive(archive_dir: Path, bucket: str) -> List[ArchiveEntry]:
    """Load all ArchiveEntry records from a given date bucket file."""
    if not isinstance(archive_dir, Path):
        archive_dir = Path(archive_dir)
    target = archive_dir / f"{bucket}.jsonl"
    if not target.exists():
        raise ArchiverError(f"Archive file not found: {target}")
    entries: List[ArchiveEntry] = []
    with target.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ArchiverError(f"Invalid JSON on line {lineno}: {exc}") from exc
            entries.append(ArchiveEntry.from_dict(data))
    return entries
