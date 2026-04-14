"""Tests for driftwatch.heatmap."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.heatmap import (
    HeatCell,
    HeatmapError,
    HeatmapReport,
    build_heatmap,
)


def _diff(field: str, kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="a", actual="b")


def _make(service: str, fields: list[str]) -> DriftResult:
    diffs = [_diff(f) for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# HeatCell
# ---------------------------------------------------------------------------

class TestHeatCell:
    def test_to_dict_keys(self):
        cell = HeatCell(service="auth", field_name="replicas", count=3)
        d = cell.to_dict()
        assert set(d.keys()) == {"service", "field", "count"}

    def test_to_dict_values(self):
        cell = HeatCell(service="auth", field_name="replicas", count=3)
        d = cell.to_dict()
        assert d["service"] == "auth"
        assert d["field"] == "replicas"
        assert d["count"] == 3


# ---------------------------------------------------------------------------
# build_heatmap
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(HeatmapError):
        build_heatmap(None)


def test_empty_list_returns_empty_report():
    report = build_heatmap([])
    assert isinstance(report, HeatmapReport)
    assert report.cells == []


def test_single_result_single_field():
    report = build_heatmap([_make("auth", ["replicas"])])
    assert len(report.cells) == 1
    assert report.cells[0].service == "auth"
    assert report.cells[0].field_name == "replicas"
    assert report.cells[0].count == 1


def test_multiple_results_same_field_accumulates():
    results = [
        _make("auth", ["replicas"]),
        _make("auth", ["replicas"]),
    ]
    report = build_heatmap(results)
    assert report.get("auth", "replicas") == 2


def test_different_services_tracked_separately():
    results = [
        _make("auth", ["replicas"]),
        _make("gateway", ["replicas"]),
    ]
    report = build_heatmap(results)
    assert report.get("auth", "replicas") == 1
    assert report.get("gateway", "replicas") == 1


def test_get_missing_returns_zero():
    report = build_heatmap([_make("auth", ["replicas"])])
    assert report.get("auth", "memory") == 0
    assert report.get("unknown", "replicas") == 0


def test_services_returns_sorted_unique():
    results = [_make("gateway", ["x"]), _make("auth", ["y"])]
    report = build_heatmap(results)
    assert report.services() == ["auth", "gateway"]


def test_fields_returns_sorted_unique():
    results = [_make("auth", ["replicas", "memory"]), _make("auth", ["replicas"])]
    report = build_heatmap(results)
    assert report.fields() == ["memory", "replicas"]


def test_hottest_returns_top_n():
    results = [
        _make("auth", ["replicas", "replicas", "memory"]),
        _make("auth", ["replicas"]),
    ]
    report = build_heatmap(results)
    top = report.hottest(1)
    assert len(top) == 1
    assert top[0].field_name == "replicas"
    assert top[0].count == 3


def test_summary_no_data():
    report = HeatmapReport(cells=[])
    assert "no drift" in report.summary()


def test_summary_with_data():
    report = build_heatmap([_make("auth", ["replicas"])])
    text = report.summary()
    assert "auth" in text
    assert "replicas" in text
