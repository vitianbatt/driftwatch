"""Tag-based grouping and filtering of drift results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from driftwatch.comparator import DriftResult


class TaggingError(Exception):
    """Raised when tagging operations fail."""


@dataclass
class TaggedResult:
    """A DriftResult decorated with a set of string tags."""

    result: DriftResult
    tags: List[str] = field(default_factory=list)

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags


def tag_results(
    results: List[DriftResult],
    tag_map: Dict[str, List[str]],
) -> List[TaggedResult]:
    """Attach tags to results based on *tag_map*.

    *tag_map* maps a service name to a list of tags.  Results whose
    service is not present in the map are tagged with an empty list.

    Raises TaggingError if *results* is None.
    """
    if results is None:
        raise TaggingError("results must not be None")
    if tag_map is None:
        raise TaggingError("tag_map must not be None")

    tagged: List[TaggedResult] = []
    for r in results:
        tags = list(tag_map.get(r.service, []))
        tagged.append(TaggedResult(result=r, tags=tags))
    return tagged


def filter_by_tag(
    tagged: List[TaggedResult],
    tag: str,
) -> List[TaggedResult]:
    """Return only those TaggedResults that carry *tag*.

    Raises TaggingError if *tag* is empty or whitespace.
    """
    if not tag or not tag.strip():
        raise TaggingError("tag must be a non-empty string")
    return [t for t in tagged if t.has_tag(tag)]


def group_by_tag(
    tagged: List[TaggedResult],
) -> Dict[str, List[TaggedResult]]:
    """Return a mapping of tag -> list of TaggedResults.

    Results that carry multiple tags appear under each relevant key.
    Results with no tags appear under the empty-string key.
    """
    groups: Dict[str, List[TaggedResult]] = {}
    for t in tagged:
        if not t.tags:
            groups.setdefault("", []).append(t)
        for tag in t.tags:
            groups.setdefault(tag, []).append(t)
    return groups
