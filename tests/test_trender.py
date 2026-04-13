"""Tests for driftwatch/trender.py."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.trender import (
    TrenderError,
    TrendPoint,
    TrendReport,
    build_trend,
)


def _diff(field: str = "replicas") -> FieldDiff:
    return FieldDiff(field=field, action="changed", expected="2", actual="3")


def _make(service: str, n_diffs: int = 0) -> DriftResult:
    diffs = [_diff(f"f{i}") for i in range(n_diffs)]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# TrendPoint
# ---------------------------------------------------------------------------

class TestTrendPoint:
    def test_to_dict_keys(self):
        pt = TrendPoint(service="auth", drift_count=3, timestamp="2024-01-01T00:00:00")
        d = pt.to_dict()
        assert set(d.keys()) == {"service", "drift_count", "timestamp"}

    def test_to_dict_values(self):
        pt = TrendPoint(service="auth", drift_count=3, timestamp="2024-01-01T00:00:00")
        d = pt.to_dict()
        assert d["service"] == "auth"
        assert d["drift_count"] == 3
        assert d["timestamp"] == "2024-01-01T00:00:00"


# ---------------------------------------------------------------------------
# TrendReport helpers
# ---------------------------------------------------------------------------

class TestTrendReport:
    def _report_with_two_points(self) -> TrendReport:
        r = TrendReport()
        r.points = [
            TrendPoint("svc", 1, "2024-01-01T00:00:00"),
            TrendPoint("svc", 4, "2024-01-02T00:00:00"),
        ]
        return r

    def test_services_sorted(self):
        r = TrendReport()
        r.points = [
            TrendPoint("zebra", 0, "t1"),
            TrendPoint("alpha", 2, "t1"),
        ]
        assert r.services() == ["alpha", "zebra"]

    def test_points_for_filters_correctly(self):
        r = self._report_with_two_points()
        r.points.append(TrendPoint("other", 0, "t3"))
        assert len(r.points_for("svc")) == 2
        assert len(r.points_for("other")) == 1

    def test_is_increasing_true(self):
        r = self._report_with_two_points()
        assert r.is_increasing("svc") is True

    def test_is_increasing_false_when_decreasing(self):
        r = TrendReport()
        r.points = [
            TrendPoint("svc", 5, "t1"),
            TrendPoint("svc", 1, "t2"),
        ]
        assert r.is_increasing("svc") is False

    def test_is_decreasing_true(self):
        r = TrendReport()
        r.points = [
            TrendPoint("svc", 5, "t1"),
            TrendPoint("svc", 1, "t2"),
        ]
        assert r.is_decreasing("svc") is True

    def test_is_decreasing_false_when_increasing(self):
        r = self._report_with_two_points()
        assert r.is_decreasing("svc") is False

    def test_single_point_not_increasing_or_decreasing(self):
        r = TrendReport()
        r.points = [TrendPoint("svc", 2, "t1")]
        assert r.is_increasing("svc") is False
        assert r.is_decreasing("svc") is False

    def test_summary_no_data(self):
        assert TrendReport().summary() == "no trend data"

    def test_summary_lists_services(self):
        r = self._report_with_two_points()
        s = r.summary()
        assert "svc" in s
        assert "increasing" in s


# ---------------------------------------------------------------------------
# build_trend
# ---------------------------------------------------------------------------

class TestBuildTrend:
    def test_none_results_raises(self):
        with pytest.raises(TrenderError):
            build_trend(None, ["t1"])

    def test_none_timestamps_raises(self):
        with pytest.raises(TrenderError):
            build_trend([[]], None)

    def test_length_mismatch_raises(self):
        with pytest.raises(TrenderError):
            build_trend([[_make("a")]], ["t1", "t2"])

    def test_empty_batches_returns_empty_report(self):
        report = build_trend([], [])
        assert report.points == []

    def test_single_batch_creates_points(self):
        batch = [_make("auth", 2), _make("payments", 0)]
        report = build_trend([batch], ["2024-01-01T00:00:00"])
        assert len(report.points) == 2

    def test_drift_count_correct(self):
        batch = [_make("auth", 3)]
        report = build_trend([batch], ["t1"])
        assert report.points[0].drift_count == 3

    def test_no_drift_count_zero(self):
        batch = [_make("auth", 0)]
        report = build_trend([batch], ["t1"])
        assert report.points[0].drift_count == 0

    def test_multiple_batches_accumulate(self):
        b1 = [_make("auth", 1)]
        b2 = [_make("auth", 3)]
        report = build_trend([b1, b2], ["t1", "t2"])
        pts = report.points_for("auth")
        assert len(pts) == 2
        assert pts[0].drift_count == 1
        assert pts[1].drift_count == 3

    def test_trend_increasing_detected(self):
        b1 = [_make("auth", 1)]
        b2 = [_make("auth", 5)]
        report = build_trend([b1, b2], ["t1", "t2"])
        assert report.is_increasing("auth") is True

    def test_timestamps_assigned(self):
        batch = [_make("auth", 1)]
        report = build_trend([batch], ["2024-06-15T12:00:00"])
        assert report.points[0].timestamp == "2024-06-15T12:00:00"
