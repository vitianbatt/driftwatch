"""Tests for driftwatch.merger."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.merger import MergerError, MergedReport, merge_results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [FieldDiff(kind="missing", field=f, expected="x", actual=None) for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# TestMergedReport
# ---------------------------------------------------------------------------

class TestMergedReport:
    def test_has_conflicts_false_when_empty(self):
        report = MergedReport(results=[], source_count=0, conflict_services=[])
        assert report.has_conflicts() is False

    def test_has_conflicts_true_when_populated(self):
        report = MergedReport(results=[], source_count=1, conflict_services=["svc-a"])
        assert report.has_conflicts() is True

    def test_summary_no_drift(self):
        report = MergedReport(results=[_make("svc-a")], source_count=1)
        text = report.summary()
        assert "1 service" in text
        assert "Clean: 1" in text

    def test_summary_with_drift(self):
        report = MergedReport(results=[_make("svc-a", ["env"])], source_count=1)
        text = report.summary()
        assert "Drifted: 1" in text

    def test_summary_mentions_conflicts(self):
        report = MergedReport(
            results=[_make("svc-a")],
            source_count=2,
            conflict_services=["svc-a"],
        )
        text = report.summary()
        assert "svc-a" in text
        assert "Conflicts" in text


# ---------------------------------------------------------------------------
# TestMergeResults
# ---------------------------------------------------------------------------

class TestMergeResults:
    def test_none_sources_raises(self):
        with pytest.raises(MergerError, match="sources must not be None"):
            merge_results(None)

    def test_none_inner_list_raises(self):
        with pytest.raises(MergerError, match="Individual source"):
            merge_results([[_make("svc-a")], None])

    def test_unsupported_strategy_raises(self):
        with pytest.raises(MergerError, match="Unsupported merge strategy"):
            merge_results([[]], strategy="first")

    def test_empty_sources_returns_empty_report(self):
        report = merge_results([])
        assert report.results == []
        assert report.source_count == 0

    def test_single_source_no_conflict(self):
        batch = [_make("svc-a"), _make("svc-b")]
        report = merge_results([batch])
        assert len(report.results) == 2
        assert report.conflict_services == []

    def test_two_sources_no_overlap(self):
        report = merge_results([[_make("svc-a")], [_make("svc-b")]])
        services = {r.service for r in report.results}
        assert services == {"svc-a", "svc-b"}
        assert report.source_count == 2

    def test_conflict_recorded_once(self):
        r1 = _make("svc-a", ["env"])
        r2 = _make("svc-a")
        report = merge_results([[r1], [r2]])
        assert report.conflict_services == ["svc-a"]

    def test_last_write_wins_on_conflict(self):
        r1 = _make("svc-a", ["env"])
        r2 = _make("svc-a")  # clean version — should win
        report = merge_results([[r1], [r2]])
        merged = {r.service: r for r in report.results}
        assert merged["svc-a"].diffs == []

    def test_three_sources_conflict_recorded_once(self):
        batches = [[_make("svc-a", ["k1"])], [_make("svc-a", ["k2"])], [_make("svc-a")]]
        report = merge_results(batches)
        assert report.conflict_services.count("svc-a") == 1
