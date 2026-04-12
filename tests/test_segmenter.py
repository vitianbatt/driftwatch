"""Tests for driftwatch.segmenter."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.segmenter import (
    SegmentRule,
    SegmentedReport,
    SegmenterError,
    segment_results,
)


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


# ---------------------------------------------------------------------------
# TestSegmentRule
# ---------------------------------------------------------------------------

class TestSegmentRule:
    def test_valid_rule_created(self):
        rule = SegmentRule(name="infra", pattern="infra_*")
        assert rule.name == "infra"
        assert rule.pattern == "infra_*"

    def test_empty_name_raises(self):
        with pytest.raises(SegmenterError, match="name"):
            SegmentRule(name="", pattern="*")

    def test_whitespace_name_raises(self):
        with pytest.raises(SegmenterError, match="name"):
            SegmentRule(name="   ", pattern="*")

    def test_empty_pattern_raises(self):
        with pytest.raises(SegmenterError, match="pattern"):
            SegmentRule(name="x", pattern="")

    def test_whitespace_pattern_raises(self):
        with pytest.raises(SegmenterError, match="pattern"):
            SegmentRule(name="x", pattern="  ")

    def test_matches_field_exact(self):
        rule = SegmentRule(name="r", pattern="timeout")
        assert rule.matches_field("timeout") is True
        assert rule.matches_field("retries") is False

    def test_matches_field_glob(self):
        rule = SegmentRule(name="r", pattern="db_*")
        assert rule.matches_field("db_host") is True
        assert rule.matches_field("db_port") is True
        assert rule.matches_field("cache_host") is False


# ---------------------------------------------------------------------------
# TestSegmentResults
# ---------------------------------------------------------------------------

class TestSegmentResults:
    def test_none_results_raises(self):
        with pytest.raises(SegmenterError):
            segment_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(SegmenterError):
            segment_results([], None)

    def test_empty_results_returns_empty_report(self):
        rules = [SegmentRule(name="net", pattern="net_*")]
        report = segment_results([], rules)
        assert report.total() == 0
        assert report.unmatched == []

    def test_no_drift_goes_to_unmatched(self):
        rules = [SegmentRule(name="net", pattern="net_*")]
        result = _make("svc-a")
        report = segment_results([result], rules)
        assert report.unmatched == [result]
        assert report.size("net") == 0

    def test_matching_result_placed_in_segment(self):
        rules = [SegmentRule(name="db", pattern="db_*")]
        result = _make("svc-a", ["db_host", "db_port"])
        report = segment_results([result], rules)
        assert report.size("db") == 1
        assert report.unmatched == []

    def test_first_matching_rule_wins(self):
        rules = [
            SegmentRule(name="first", pattern="timeout"),
            SegmentRule(name="second", pattern="timeout"),
        ]
        result = _make("svc-a", ["timeout"])
        report = segment_results([result], rules)
        assert report.size("first") == 1
        assert report.size("second") == 0

    def test_multiple_results_split_across_segments(self):
        rules = [
            SegmentRule(name="net", pattern="net_*"),
            SegmentRule(name="db", pattern="db_*"),
        ]
        r1 = _make("svc-a", ["net_timeout"])
        r2 = _make("svc-b", ["db_host"])
        r3 = _make("svc-c", ["unrelated"])
        report = segment_results([r1, r2, r3], rules)
        assert report.size("net") == 1
        assert report.size("db") == 1
        assert len(report.unmatched) == 1
        assert report.total() == 3

    def test_segment_names_sorted(self):
        rules = [
            SegmentRule(name="zebra", pattern="z_*"),
            SegmentRule(name="alpha", pattern="a_*"),
        ]
        report = segment_results([], rules)
        assert report.segment_names() == ["alpha", "zebra"]
