"""Labeler: attach arbitrary key-value labels to DriftResults for downstream routing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class LabelerError(Exception):
    """Raised when labeling fails."""


@dataclass
class LabeledResult:
    """A DriftResult decorated with a labels dict."""

    result: DriftResult
    labels: Dict[str, str] = field(default_factory=dict)

    def has_label(self, key: str) -> bool:
        """Return True if *key* is present in labels."""
        return key in self.labels

    def get_label(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Return the value for *key*, or *default* if absent."""
        return self.labels.get(key, default)

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "has_drift": self.result.has_drift,
            "drifted_fields": list(self.result.drifted_fields),
            "labels": dict(self.labels),
        }


def label_results(
    results: List[DriftResult],
    label_map: Dict[str, Dict[str, str]],
) -> List[LabeledResult]:
    """Attach labels from *label_map* to each result.

    *label_map* maps service name -> {label_key: label_value}.
    Services not present in the map receive an empty labels dict.

    Raises:
        LabelerError: if *results* or *label_map* is None.
    """
    if results is None:
        raise LabelerError("results must not be None")
    if label_map is None:
        raise LabelerError("label_map must not be None")

    labeled: List[LabeledResult] = []
    for r in results:
        labels = dict(label_map.get(r.service, {}))
        labeled.append(LabeledResult(result=r, labels=labels))
    return labeled


def filter_by_label(labeled: List[LabeledResult], key: str, value: str) -> List[LabeledResult]:
    """Return only results whose label *key* equals *value*."""
    if labeled is None:
        raise LabelerError("labeled must not be None")
    return [lr for lr in labeled if lr.labels.get(key) == value]
