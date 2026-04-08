"""Deep diff utilities for comparing spec vs live config values."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class DiffError(Exception):
    """Raised when diffing encounters an unexpected error."""


@dataclass
class FieldDiff:
    """Represents a single field-level difference."""

    key: str
    expected: Any
    actual: Any
    diff_type: str  # 'missing', 'extra', 'changed'

    def __str__(self) -> str:
        if self.diff_type == "missing":
            return f"  - {self.key}: expected {self.expected!r}, not present in live config"
        if self.diff_type == "extra":
            return f"  + {self.key}: not in spec, found {self.actual!r} in live config"
        return f"  ~ {self.key}: expected {self.expected!r}, got {self.actual!r}"


def deep_diff(
    spec: dict[str, Any],
    live: dict[str, Any],
    *,
    path: str = "",
    ignore_extra: bool = False,
) -> list[FieldDiff]:
    """Recursively diff *spec* against *live*, returning a list of FieldDiff objects.

    Args:
        spec: The declared specification dictionary.
        live: The live configuration dictionary.
        path: Dot-separated key path prefix used during recursion.
        ignore_extra: When True, keys present in *live* but absent from *spec* are ignored.

    Returns:
        A (possibly empty) list of :class:`FieldDiff` instances.
    """
    if not isinstance(spec, dict) or not isinstance(live, dict):
        raise DiffError(
            f"Both spec and live must be dicts at path '{path or '(root)'}'; "
            f"got {type(spec).__name__} and {type(live).__name__}"
        )

    diffs: list[FieldDiff] = []

    for key, expected in spec.items():
        full_key = f"{path}.{key}" if path else key
        if key not in live:
            diffs.append(FieldDiff(key=full_key, expected=expected, actual=None, diff_type="missing"))
        elif isinstance(expected, dict) and isinstance(live[key], dict):
            diffs.extend(deep_diff(expected, live[key], path=full_key, ignore_extra=ignore_extra))
        elif live[key] != expected:
            diffs.append(FieldDiff(key=full_key, expected=expected, actual=live[key], diff_type="changed"))

    if not ignore_extra:
        for key in live:
            if key not in spec:
                full_key = f"{path}.{key}" if path else key
                diffs.append(FieldDiff(key=full_key, expected=None, actual=live[key], diff_type="extra"))

    return diffs


def format_diffs(diffs: list[FieldDiff]) -> str:
    """Format a list of FieldDiff objects into a human-readable summary string.

    Args:
        diffs: The list of :class:`FieldDiff` instances to format.

    Returns:
        A multi-line string summarising all differences, or a message indicating
        no differences were found.
    """
    if not diffs:
        return "No differences found."
    lines = [f"{len(diffs)} difference(s) found:"]
    lines.extend(str(d) for d in diffs)
    return "\n".join(lines)
