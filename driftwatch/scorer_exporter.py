"""Export scored drift results to JSONL or CSV formats."""

from __future__ import annotations

import csv
import io
import json
from enum import Enum
from typing import List

from driftwatch.scorer import ScoredResult


class ScorerExporterError(Exception):
    """Raised when export fails."""


class ExportFormat(str, Enum):
    JSONL = "jsonl"
    CSV = "csv"


def _result_to_dict(result: ScoredResult) -> dict:
    return {
        "service": result.service,
        "score": result.score,
        "has_drift": result.has_drift,
        "drift_fields": [d.field for d in result.diffs] if result.diffs else [],
    }


def export_jsonl(results: List[ScoredResult]) -> str:
    """Serialize a list of ScoredResults to JSONL string."""
    if results is None:
        raise ScorerExporterError("results must not be None")
    lines = [json.dumps(_result_to_dict(r)) for r in results]
    return "\n".join(lines)


def export_csv(results: List[ScoredResult]) -> str:
    """Serialize a list of ScoredResults to CSV string."""
    if results is None:
        raise ScorerExporterError("results must not be None")
    buf = io.StringIO()
    fieldnames = ["service", "score", "has_drift", "drift_fields"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in results:
        row = _result_to_dict(r)
        row["drift_fields"] = "|".join(row["drift_fields"])
        writer.writerow(row)
    return buf.getvalue().rstrip("\r\n")


def export_scored_results(
    results: List[ScoredResult],
    fmt: ExportFormat = ExportFormat.JSONL,
) -> str:
    """Export scored results in the requested format."""
    if fmt == ExportFormat.JSONL:
        return export_jsonl(results)
    if fmt == ExportFormat.CSV:
        return export_csv(results)
    raise ScorerExporterError(f"Unsupported format: {fmt}")
