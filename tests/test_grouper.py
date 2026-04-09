"""Tests for driftwatch.grouper."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.grouper import GroupBy, GroupedReport, GrouperError, group_results


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# GroupedReport helpers
# ---------------------------------------------------------------------------

class TestGroupedReport:
    def test_group_names_sorted(self):
        report = GroupedReport(dimension="service", groups={"z": [], "a": [], "m": []})
        assert report.group_names() == ["a", "m", "z"]

    def test_size_missing_group_returns_zero(self):
        report = GroupedReport(dimension="service")
        assert report.size("nonexistent") == 0

    def test_total_sums_all_groups(self):
        r = _make("svc")
        report = GroupedReport(dimension="service", groups={"a": [r, r], "b": [r]})
        assert report.total() == 3

    def test_summary_contains_dimension(self):
        report = GroupedReport(dimension="severity")
        assert "severity" in report.summary()


# ---------------------------------------------------------------------------
# group_results — by SERVICE
# ---------------------------------------------------------------------------

class TestGroupByService:
    def test_single_service(self):
        results = [_make("auth"), _make("auth")]
        report = group_results(results, GroupBy.SERVICE)
        assert report.group_names() == ["auth"]
        assert report.size("auth") == 2

    def test_multiple_services(self):
        results = [_make("auth"), _make("billing"), _make("auth")]
        report = group_results(results, GroupBy.SERVICE)
        assert set(report.group_names()) == {"auth", "billing"}
        assert report.size("auth") == 2
        assert report.size("billing") == 1

    def test_empty_results_returns_empty_groups(self):
        report = group_results([], GroupBy.SERVICE)
        assert report.groups == {}
        assert report.total() == 0

    def test_none_results_raises(self):
        with pytest.raises(GrouperError):
            group_results(None, GroupBy.SERVICE)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# group_results — by SEVERITY
# ---------------------------------------------------------------------------

class TestGroupBySeverity:
    def test_no_diffs_is_low(self):
        report = group_results([_make("svc", [])], GroupBy.SEVERITY)
        assert "low" in report.group_names()

    def test_two_diffs_is_medium(self):
        report = group_results([_make("svc", ["a", "b"])], GroupBy.SEVERITY)
        assert "medium" in report.group_names()

    def test_three_diffs_is_high(self):
        report = group_results([_make("svc", ["a", "b", "c"])], GroupBy.SEVERITY)
        assert "high" in report.group_names()


# ---------------------------------------------------------------------------
# group_results — by TAG
# ---------------------------------------------------------------------------

class TestGroupByTag:
    def test_tag_map_required(self):
        with pytest.raises(GrouperError, match="tag_map"):
            group_results([_make("svc")], GroupBy.TAG)

    def test_known_service_gets_tag(self):
        tag_map = {"auth": "core"}
        report = group_results([_make("auth")], GroupBy.TAG, tag_map=tag_map)
        assert "core" in report.group_names()

    def test_unknown_service_is_untagged(self):
        report = group_results([_make("unknown")], GroupBy.TAG, tag_map={})
        assert "untagged" in report.group_names()

    def test_mixed_tagged_and_untagged(self):
        tag_map = {"auth": "core"}
        results = [_make("auth"), _make("billing")]
        report = group_results(results, GroupBy.TAG, tag_map=tag_map)
        assert report.size("core") == 1
        assert report.size("untagged") == 1
