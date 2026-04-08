"""Comparator module for detecting drift between spec and live config."""

from dataclasses import dataclass, field
from typing import Any


class DriftCompareError(Exception):
    """Raised when comparison cannot be performed."""


@dataclass
class DriftResult:
    """Represents the drift result for a single service spec."""

    service: str
    missing_keys: list[str] = field(default_factory=list)
    extra_keys: list[str] = field(default_factory=list)
    mismatched_values: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def has_drift(self) -> bool:
        return bool(self.missing_keys or self.extra_keys or self.mismatched_values)

    def summary(self) -> str:
        if not self.has_drift:
            return f"[{self.service}] No drift detected."
        lines = [f"[{self.service}] Drift detected:"]
        for key in self.missing_keys:
            lines.append(f"  - missing key: {key}")
        for key in self.extra_keys:
            lines.append(f"  + extra key:   {key}")
        for key, diff in self.mismatched_values.items():
            lines.append(f"  ~ {key}: expected={diff['expected']!r}, actual={diff['actual']!r}")
        return "\n".join(lines)


def compare(spec: dict[str, Any], live: dict[str, Any], service: str = "unknown") -> DriftResult:
    """Compare a declared spec dict against a live config dict.

    Args:
        spec: The declared specification (source of truth).
        live: The live/deployed configuration.
        service: Name of the service for reporting.

    Returns:
        A DriftResult describing any differences found.
    """
    if not isinstance(spec, dict) or not isinstance(live, dict):
        raise DriftCompareError("Both spec and live config must be dictionaries.")

    result = DriftResult(service=service)

    spec_keys = set(spec.keys())
    live_keys = set(live.keys())

    result.missing_keys = sorted(spec_keys - live_keys)
    result.extra_keys = sorted(live_keys - spec_keys)

    for key in spec_keys & live_keys:
        if spec[key] != live[key]:
            result.mismatched_values[key] = {"expected": spec[key], "actual": live[key]}

    return result
