"""Tests for driftwatch/sorter.py."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.sorter import SortBy, SortedReport, SorterError, sort_results


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


# ---------------------------------------------------------------------------
# TestSortedReport
# ---------------------------------------------------------------------------

class TestSortedReport:
    def test_len_reflects_results(self):
        report = SortedReport(
            results=[_make("a"), _make("b")],
            sort_by=SortBy.SERVICE,
            ascending=True,
        )
        assert len(report) == 2

    def test_service_names_order_preserved(self):
        report = SortedReport(
            results=[_make("zebra"), _make("alpha")],
            sort_by=SortBy.SERVICE,
            ascending=True,
        )
        assert report.service_names() == ["zebra", "alpha"]


# ---------------------------------------------------------------------------
# TestSortResults — by service
# ---------------------------------------------------------------------------

class TestSortByService:
    def test_ascending_alphabetical(self):
        results = [_make("zebra"), _make("alpha"), _make("mango")]
        report = sort_results(results, SortBy.SERVICE, ascending=True)
        assert report.service_names() == ["alpha", "mango", "zebra"]

    def test_descending_alphabetical(self):
        results = [_make("zebra"), _make("alpha"), _make("mango")]
        report = sort_results(results, SortBy.SERVICE, ascending=False)
        assert report.service_names() == ["zebra", "mango", "alpha"]

    def test_case_insensitive(self):
        results = [_make("Zebra"), _make("alpha")]
        report = sort_results(results, SortBy.SERVICE, ascending=True)
        assert report.service_names()[0] == "alpha"


# ---------------------------------------------------------------------------
# TestSortResults — by drift count
# ---------------------------------------------------------------------------

class TestSortByDriftCount:
    def test_ascending_fewest_first(self):
        results = [
            _make("svc-a", ["x", "y", "z"]),
            _make("svc-b", []),
            _make("svc-c", ["p"]),
        ]
        report = sort_results(results, SortBy.DRIFT_COUNT, ascending=True)
        assert report.service_names() == ["svc-b", "svc-c", "svc-a"]

    def test_descending_most_first(self):
        results = [
            _make("svc-a", ["x", "y", "z"]),
            _make("svc-b", []),
            _make("svc-c", ["p"]),
        ]
        report = sort_results(results, SortBy.DRIFT_COUNT, ascending=False)
        assert report.service_names() == ["svc-a", "svc-c", "svc-b"]

    def test_severity_alias_same_as_drift_count(self):
        results = [_make("a", ["f1", "f2"]), _make("b", ["f1"])]
        by_count = sort_results(results, SortBy.DRIFT_COUNT, ascending=False)
        by_sev = sort_results(results, SortBy.SEVERITY, ascending=False)
        assert by_count.service_names() == by_sev.service_names()


# ---------------------------------------------------------------------------
# TestSortResults — edge cases
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(SorterError, match="must not be None"):
        sort_results(None)  # type: ignore[arg-type]


def test_empty_list_returns_empty_report():
    report = sort_results([], SortBy.SERVICE)
    assert len(report) == 0
    assert report.service_names() == []


def test_sort_by_stored_on_report():
    report = sort_results([_make("svc")], SortBy.DRIFT_COUNT, ascending=False)
    assert report.sort_by == SortBy.DRIFT_COUNT
    assert report.ascending is False
