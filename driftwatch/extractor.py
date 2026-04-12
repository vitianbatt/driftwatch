"""extractor.py — extract specific fields from drift results for downstream processing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class ExtractorError(Exception):
    """Raised when field extraction fails."""


@dataclass
class ExtractedResult:
    service: str
    extracted: Dict[str, List[FieldDiff]] = field(default_factory=dict)

    def has_drift(self) -> bool:
        return any(self.extracted.values())

    def field_names(self) -> List[str]:
        return sorted(self.extracted.keys())

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "fields": {
                k: [str(d) for d in v] for k, v in self.extracted.items()
            },
        }


@dataclass
class ExtractionReport:
    results: List[ExtractedResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def get(self, service: str) -> Optional[ExtractedResult]:
        for r in self.results:
            if r.service == service:
                return r
        return None


def extract_fields(
    results: List[DriftResult],
    fields: List[str],
) -> ExtractionReport:
    """Extract only the specified drift fields from each result."""
    if results is None:
        raise ExtractorError("results must not be None")
    if fields is None:
        raise ExtractorError("fields list must not be None")
    if not fields:
        raise ExtractorError("fields list must not be empty")

    clean_fields = []
    for f in fields:
        if not f or not f.strip():
            raise ExtractorError("field name must not be blank")
        clean_fields.append(f.strip())

    extracted_results: List[ExtractedResult] = []
    for result in results:
        bucket: Dict[str, List[FieldDiff]] = {}
        for fd in (result.diffs or []):
            if fd.field in clean_fields:
                bucket.setdefault(fd.field, []).append(fd)
        extracted_results.append(
            ExtractedResult(service=result.service, extracted=bucket)
        )

    return ExtractionReport(results=extracted_results)
