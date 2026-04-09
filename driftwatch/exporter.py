"""Export drift audit logs to various output formats (JSONL, CSV)."""

from __future__ import annotations

import csv
import io
import json
from enum import Enum
from pathlib import Path
from typing import Iterable

from driftwatch.auditor import AuditEntry


class ExportError(Exception):
    """Raised when an export operation fails."""


class ExportFormat(str, Enum):
    JSONL = "jsonl"
    CSV = "csv"


_CSV_FIELDS = ["timestamp", "service", "has_drift", "drift_fields", "spec_source"]


def _entry_to_csv_row(entry: AuditEntry) -> dict:
    return {
        "timestamp": entry.timestamp,
        "service": entry.service,
        "has_drift": str(entry.has_drift).lower(),
        "drift_fields": "|".join(entry.drift_fields),
        "spec_source": entry.spec_source or "",
    }


def export_jsonl(entries: Iterable[AuditEntry]) -> str:
    """Serialize entries to newline-delimited JSON."""
    lines = [json.dumps(e.to_dict()) for e in entries]
    return "\n".join(lines) + ("\n" if lines else "")


def export_csv(entries: Iterable[AuditEntry]) -> str:
    """Serialize entries to CSV with a header row."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for entry in entries:
        writer.writerow(_entry_to_csv_row(entry))
    return buf.getvalue()


def export_entries(
    entries: Iterable[AuditEntry],
    fmt: ExportFormat,
    dest: Path | None = None,
) -> str:
    """Export *entries* in *fmt* format, optionally writing to *dest*.

    Returns the serialized string regardless of whether *dest* is given.
    """
    entries = list(entries)
    if fmt is ExportFormat.JSONL:
        output = export_jsonl(entries)
    elif fmt is ExportFormat.CSV:
        output = export_csv(entries)
    else:
        raise ExportError(f"Unsupported export format: {fmt!r}")

    if dest is not None:
        try:
            dest.write_text(output, encoding="utf-8")
        except OSError as exc:
            raise ExportError(f"Failed to write export to {dest}: {exc}") from exc

    return output
