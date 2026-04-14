"""timeline.py — builds a chronological drift timeline for a service."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


class TimelineError(Exception):
    """Raised when timeline construction fails."""


@dataclass
class TimelineEvent:
    """A single point-in-time drift observation for a service."""

    timestamp: str
    service: str
    drifted_fields: List[str]
    resolved_fields: List[str] = field(default_factory=list)

    def has_drift(self) -> bool:
        return bool(self.drifted_fields)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "drifted_fields": sorted(self.drifted_fields),
            "resolved_fields": sorted(self.resolved_fields),
            "has_drift": self.has_drift(),
        }


@dataclass
class Timeline:
    """Ordered sequence of drift events for a single service."""

    service: str
    events: List[TimelineEvent] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.events)

    def drift_events(self) -> List[TimelineEvent]:
        return [e for e in self.events if e.has_drift()]

    def latest(self) -> Optional[TimelineEvent]:
        return self.events[-1] if self.events else None

    def summary(self) -> str:
        total = len(self.events)
        drifted = len(self.drift_events())
        if total == 0:
            return f"{self.service}: no events recorded"
        return f"{self.service}: {drifted}/{total} events with drift"


def build_timeline(service: str, events: List[dict]) -> Timeline:
    """Construct a Timeline from a list of raw event dicts.

    Each dict must have 'timestamp', 'drifted_fields', and optionally
    'resolved_fields'.
    """
    if events is None:
        raise TimelineError("events must not be None")
    if not service or not service.strip():
        raise TimelineError("service name must not be empty")

    ordered: List[TimelineEvent] = []
    for raw in events:
        if "timestamp" not in raw:
            raise TimelineError("each event must contain a 'timestamp' key")
        if "drifted_fields" not in raw:
            raise TimelineError("each event must contain a 'drifted_fields' key")
        ordered.append(
            TimelineEvent(
                timestamp=raw["timestamp"],
                service=service,
                drifted_fields=list(raw["drifted_fields"]),
                resolved_fields=list(raw.get("resolved_fields", [])),
            )
        )

    ordered.sort(key=lambda e: e.timestamp)
    return Timeline(service=service, events=ordered)
