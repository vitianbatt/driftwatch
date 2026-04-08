"""Reporter module for formatting and outputting drift results."""

from __future__ import annotations

from enum import Enum
from typing import List

from driftwatch.comparator import DriftResult


class OutputFormat(str, Enum):
    TEXT = "text"
    JSON = "json"


class ReportError(Exception):
    """Raised when report generation fails."""


def _format_text(results: List[DriftResult]) -> str:
    """Render drift results as human-readable text."""
    if not results:
        return "No drift detected across all services.\n"

    lines: List[str] = []
    drifted = [r for r in results if r.has_drift]
    clean = [r for r in results if not r.has_drift]

    if drifted:
        lines.append(f"Drift detected in {len(drifted)} service(s):\n")
        for result in drifted:
            lines.append(f"  [{result.service_name}]")
            for key in result.missing_keys:
                lines.append(f"    - MISSING   : {key}")
            for key, (expected, actual) in result.mismatched_keys.items():
                lines.append(f"    - MISMATCH  : {key} (expected={expected!r}, actual={actual!r})")
            lines.append("")

    if clean:
        clean_names = ", ".join(r.service_name for r in clean)
        lines.append(f"No drift: {clean_names}")

    return "\n".join(lines)


def _format_json(results: List[DriftResult]) -> str:
    """Render drift results as JSON."""
    import json

    payload = [
        {
            "service": r.service_name,
            "has_drift": r.has_drift,
            "missing_keys": list(r.missing_keys),
            "mismatched_keys": {
                k: {"expected": exp, "actual": act}
                for k, (exp, act) in r.mismatched_keys.items()
            },
        }
        for r in results
    ]
    return json.dumps(payload, indent=2)


def generate_report(
    results: List[DriftResult],
    fmt: OutputFormat = OutputFormat.TEXT,
) -> str:
    """Generate a formatted report string from a list of DriftResult objects.

    Args:
        results: Drift comparison results to report on.
        fmt: Desired output format (text or json).

    Returns:
        Formatted report as a string.

    Raises:
        ReportError: If an unsupported format is requested.
    """
    if fmt == OutputFormat.TEXT:
        return _format_text(results)
    if fmt == OutputFormat.JSON:
        return _format_json(results)
    raise ReportError(f"Unsupported output format: {fmt!r}")
