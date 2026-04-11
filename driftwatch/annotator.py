"""Annotator: attach free-form notes to drift results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class AnnotatorError(Exception):
    """Raised when annotation fails."""


@dataclass
class AnnotatedResult:
    """A DriftResult paired with zero or more annotation strings."""

    result: DriftResult
    notes: List[str] = field(default_factory=list)

    def has_notes(self) -> bool:
        return bool(self.notes)

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "has_drift": self.result.has_drift,
            "drift_fields": self.result.drift_fields,
            "notes": list(self.notes),
        }


def annotate_results(
    results: List[DriftResult],
    note_map: Dict[str, List[str]],
) -> List[AnnotatedResult]:
    """Attach notes from *note_map* (keyed by service name) to each result.

    Results whose service has no entry in *note_map* receive an empty notes
    list.  Raises *AnnotatorError* if either argument is None.
    """
    if results is None:
        raise AnnotatorError("results must not be None")
    if note_map is None:
        raise AnnotatorError("note_map must not be None")

    annotated: List[AnnotatedResult] = []
    for r in results:
        notes = list(note_map.get(r.service, []))
        annotated.append(AnnotatedResult(result=r, notes=notes))
    return annotated


def filter_by_note(
    annotated: List[AnnotatedResult],
    keyword: str,
) -> List[AnnotatedResult]:
    """Return only results whose notes contain *keyword* (case-insensitive)."""
    if annotated is None:
        raise AnnotatorError("annotated list must not be None")
    if not keyword or not keyword.strip():
        raise AnnotatorError("keyword must be a non-empty string")
    kw = keyword.lower()
    return [a for a in annotated if any(kw in n.lower() for n in a.notes)]
