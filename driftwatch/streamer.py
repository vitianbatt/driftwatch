"""Stream drift results to an output sink in real time."""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Callable, Iterable, IO

from driftwatch.comparator import DriftResult


class StreamerError(Exception):
    """Raised when streaming fails."""


@dataclass
class StreamConfig:
    format: str = "jsonl"          # "jsonl" or "text"
    out: IO[str] = field(default_factory=lambda: sys.stdout)
    flush_each: bool = True

    def __post_init__(self) -> None:
        if self.format not in ("jsonl", "text"):
            raise StreamerError(
                f"Unsupported stream format '{self.format}'; use 'jsonl' or 'text'."
            )


def _result_to_jsonl(result: DriftResult) -> str:
    payload = {
        "service": result.service,
        "has_drift": result.has_drift,
        "missing_keys": result.missing_keys,
        "extra_keys": result.extra_keys,
        "changed_keys": result.changed_keys,
    }
    return json.dumps(payload)


def _result_to_text(result: DriftResult) -> str:
    if not result.has_drift:
        return f"[OK]    {result.service}"
    parts = []
    if result.missing_keys:
        parts.append(f"missing={result.missing_keys}")
    if result.extra_keys:
        parts.append(f"extra={result.extra_keys}")
    if result.changed_keys:
        parts.append(f"changed={result.changed_keys}")
    detail = ", ".join(parts)
    return f"[DRIFT] {result.service} \u2014 {detail}"


_FORMATTERS: dict[str, Callable[[DriftResult], str]] = {
    "jsonl": _result_to_jsonl,
    "text": _result_to_text,
}


def stream_results(
    results: Iterable[DriftResult],
    config: StreamConfig | None = None,
) -> int:
    """Write each result to *config.out* immediately.

    Returns the number of results written.

    Raises:
        StreamerError: If writing to the output stream fails.
    """
    if config is None:
        config = StreamConfig()

    formatter = _FORMATTERS[config.format]
    count = 0
    for result in results:
        line = formatter(result)
        try:
            config.out.write(line + "\n")
            if config.flush_each:
                config.out.flush()
        except OSError as exc:
            raise StreamerError(
                f"Failed to write result for service '{result.service}': {exc}"
            ) from exc
        count += 1
    return count
