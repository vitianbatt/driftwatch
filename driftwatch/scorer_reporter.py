"""scorer_reporter.py — formats ScoredReport results into human-readable or JSON output."""
from __future__ import annotations

from enum import Enum
from typing import List

from driftwatch.scorer import ScoredReport, ScoredResult


class ScorerReporterError(Exception):
    """Raised when report generation fails."""


class ReportFormat(str, Enum):
    TEXT = "text"
    JSON = "json"


def _result_to_text(result: ScoredResult) -> str:
    drift_label = "DRIFT" if result.score > 0 else "OK"
    line = f"  [{drift_label}] {result.service}  score={result.score}  priority={result.priority.value}"
    if result.diffs:
        field_list = ", ".join(d.field for d in result.diffs)
        line += f"  fields=({field_list})"
    return line


def _format_text(report: ScoredReport) -> str:
    if not report.results:
        return "No scored results."
    lines = [f"Scored Report — {len(report.results)} service(s)  avg_score={report.average:.2f}"]
    for r in report.results:
        lines.append(_result_to_text(r))
    return "\n".join(lines)


def _format_json(report: ScoredReport) -> str:
    import json
    payload = {
        "average_score": report.average,
        "total": len(report.results),
        "results": [r.to_dict() for r in report.results],
    }
    return json.dumps(payload, indent=2)


def generate_scorer_report(report: ScoredReport, fmt: ReportFormat = ReportFormat.TEXT) -> str:
    """Return a formatted string representation of *report*."""
    if report is None:
        raise ScorerReporterError("report must not be None")
    if fmt == ReportFormat.JSON:
        return _format_json(report)
    if fmt == ReportFormat.TEXT:
        return _format_text(report)
    raise ScorerReporterError(f"Unsupported format: {fmt}")
