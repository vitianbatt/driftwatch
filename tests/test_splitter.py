"""Tests for driftwatch/splitter.py."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.splitter import SplitReport, SplitterError, split_results


def _make(service: str, drifted: bool = False) -> DriftResult:
    diffs = [{"field": "replicas"}] if drifted else []
    return DriftResult(service=service, diffs=diffs)


ROUTING = {
    "team-a": ["auth", "billing"],
    "team-b": ["gateway", "search"],
}


# ---------------------------------------------------------------------------
# TestSplitReport helpers
# ---------------------------------------------------------------------------

class TestSplitReport:
    def test_partition_names_sorted(self):
        report = SplitReport(partitions={"z": [], "a": []}, unmatched=[])
        assert report.partition_names() == ["a", "z"]

    def test_size_existing_partition(self):
        r = _make("auth")
        report = SplitReport(partitions={"team-a": [r]}, unmatched=[])
        assert report.size("team-a") == 1

    def test_size_missing_partition_returns_zero(self):
        report = SplitReport(partitions={}, unmatched=[])
        assert report.size("ghost") == 0

    def test_total_includes_unmatched(self):
        report = SplitReport(
            partitions={"a": [_make("x"), _make("y")]},
            unmatched=[_make("z")],
        )
        assert report.total() == 3

    def test_summary_lists_partitions(self):
        report = SplitReport(
            partitions={"team-a": [_make("auth")], "team-b": []},
            unmatched=[],
        )
        text = report.summary()
        assert "team-a: 1 result(s)" in text
        assert "team-b: 0 result(s)" in text

    def test_summary_shows_unmatched(self):
        report = SplitReport(partitions={}, unmatched=[_make("orphan")])
        assert "unmatched: 1 result(s)" in report.summary()

    def test_summary_empty_returns_no_results(self):
        report = SplitReport(partitions={}, unmatched=[])
        assert report.summary() == "no results"


# ---------------------------------------------------------------------------
# TestSplitResults
# ---------------------------------------------------------------------------

class TestSplitResults:
    def test_none_results_raises(self):
        with pytest.raises(SplitterError, match="results"):
            split_results(None, ROUTING)

    def test_none_routing_map_raises(self):
        with pytest.raises(SplitterError, match="routing_map"):
            split_results([], None)

    def test_empty_routing_map_raises(self):
        with pytest.raises(SplitterError, match="empty"):
            split_results([], {})

    def test_blank_partition_name_raises(self):
        with pytest.raises(SplitterError, match="non-empty"):
            split_results([], {"  ": ["auth"]})

    def test_duplicate_service_raises(self):
        bad_map = {"a": ["auth"], "b": ["auth"]}
        with pytest.raises(SplitterError, match="multiple partitions"):
            split_results([_make("auth")], bad_map)

    def test_matched_services_routed_correctly(self):
        results = [_make("auth"), _make("gateway"), _make("billing")]
        report = split_results(results, ROUTING)
        assert report.size("team-a") == 2
        assert report.size("team-b") == 1
        assert len(report.unmatched) == 0

    def test_unmatched_service_goes_to_unmatched(self):
        results = [_make("unknown-svc")]
        report = split_results(results, ROUTING)
        assert len(report.unmatched) == 1
        assert report.size("team-a") == 0

    def test_default_partition_catches_unmatched(self):
        results = [_make("unknown-svc")]
        report = split_results(results, ROUTING, default_partition="catch-all")
        assert report.size("catch-all") == 1
        assert len(report.unmatched) == 0

    def test_empty_results_returns_empty_partitions(self):
        report = split_results([], ROUTING)
        assert report.total() == 0
        assert report.partition_names() == ["team-a", "team-b"]

    def test_total_matches_input_length(self):
        results = [_make("auth"), _make("gateway"), _make("orphan")]
        report = split_results(results, ROUTING)
        assert report.total() == 3
