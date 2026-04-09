"""Tests for driftwatch.tagging."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.tagging import (
    TaggingError,
    TaggedResult,
    filter_by_tag,
    group_by_tag,
    tag_results,
)


def _make(service: str, drifted_fields: list | None = None) -> DriftResult:
    return DriftResult(
        service=service,
        drifted_fields=drifted_fields or [],
        missing_keys=[],
        extra_keys=[],
    )


class TestTagResults:
    def test_empty_results_returns_empty(self):
        assert tag_results([], {}) == []

    def test_none_results_raises(self):
        with pytest.raises(TaggingError, match="results must not be None"):
            tag_results(None, {})

    def test_none_tag_map_raises(self):
        with pytest.raises(TaggingError, match="tag_map must not be None"):
            tag_results([], None)

    def test_known_service_gets_tags(self):
        r = _make("auth")
        tagged = tag_results([r], {"auth": ["critical", "prod"]})
        assert len(tagged) == 1
        assert tagged[0].tags == ["critical", "prod"]

    def test_unknown_service_gets_empty_tags(self):
        r = _make("unknown-svc")
        tagged = tag_results([r], {"auth": ["critical"]})
        assert tagged[0].tags == []

    def test_tags_are_independent_copies(self):
        original = ["prod"]
        r = _make("svc")
        tagged = tag_results([r], {"svc": original})
        tagged[0].tags.append("extra")
        assert original == ["prod"]


class TestFilterByTag:
    def test_empty_tag_raises(self):
        with pytest.raises(TaggingError, match="non-empty"):
            filter_by_tag([], "")

    def test_whitespace_tag_raises(self):
        with pytest.raises(TaggingError, match="non-empty"):
            filter_by_tag([], "   ")

    def test_returns_matching_results(self):
        t1 = TaggedResult(result=_make("a"), tags=["prod"])
        t2 = TaggedResult(result=_make("b"), tags=["staging"])
        out = filter_by_tag([t1, t2], "prod")
        assert len(out) == 1
        assert out[0].result.service == "a"

    def test_no_match_returns_empty(self):
        t1 = TaggedResult(result=_make("a"), tags=["prod"])
        assert filter_by_tag([t1], "staging") == []


class TestGroupByTag:
    def test_empty_list_returns_empty_dict(self):
        assert group_by_tag([]) == {}

    def test_untagged_results_under_empty_key(self):
        t = TaggedResult(result=_make("svc"), tags=[])
        groups = group_by_tag([t])
        assert "" in groups
        assert groups[""][0].result.service == "svc"

    def test_multi_tag_appears_in_each_group(self):
        t = TaggedResult(result=_make("svc"), tags=["prod", "critical"])
        groups = group_by_tag([t])
        assert "prod" in groups
        assert "critical" in groups
        assert groups["prod"][0] is t
        assert groups["critical"][0] is t

    def test_multiple_services_grouped_correctly(self):
        t1 = TaggedResult(result=_make("a"), tags=["prod"])
        t2 = TaggedResult(result=_make("b"), tags=["prod", "eu"])
        groups = group_by_tag([t1, t2])
        assert len(groups["prod"]) == 2
        assert len(groups["eu"]) == 1
