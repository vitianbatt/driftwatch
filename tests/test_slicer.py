"""Tests for driftwatch.slicer."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.slicer import (
    SliceConfig,
    SlicedReport,
    SlicedResult,
    SlicerError,
    slice_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _diff(field: str, kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="a", actual="b")


def _make(service: str, fields: list[str]) -> DriftResult:
    return DriftResult(service=service, diffs=[_diff(f) for f in fields])


# ---------------------------------------------------------------------------
# SliceConfig
# ---------------------------------------------------------------------------

class TestSliceConfig:
    def test_valid_config_created(self):
        cfg = SliceConfig(fields=["replicas", "image"])
        assert cfg.fields == ["replicas", "image"]

    def test_none_fields_raises(self):
        with pytest.raises(SlicerError):
            SliceConfig(fields=None)  # type: ignore[arg-type]

    def test_empty_field_name_raises(self):
        with pytest.raises(SlicerError):
            SliceConfig(fields=["replicas", ""])

    def test_whitespace_field_name_raises(self):
        with pytest.raises(SlicerError):
            SliceConfig(fields=["  "])


# ---------------------------------------------------------------------------
# SlicedResult
# ---------------------------------------------------------------------------

class TestSlicedResult:
    def test_has_drift_false_when_empty(self):
        r = SlicedResult(service="svc", kept=[], omitted_count=0)
        assert r.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        r = SlicedResult(service="svc", kept=[_diff("image")], omitted_count=0)
        assert r.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        r = SlicedResult(service="svc", kept=[_diff("image")], omitted_count=2)
        d = r.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "kept_fields", "omitted_count"}

    def test_to_dict_values(self):
        r = SlicedResult(service="auth", kept=[_diff("replicas")], omitted_count=3)
        d = r.to_dict()
        assert d["service"] == "auth"
        assert d["kept_fields"] == ["replicas"]
        assert d["omitted_count"] == 3
        assert d["has_drift"] is True


# ---------------------------------------------------------------------------
# slice_results
# ---------------------------------------------------------------------------

class TestSliceResults:
    def test_none_results_raises(self):
        with pytest.raises(SlicerError):
            slice_results(None, SliceConfig(fields=["image"]))  # type: ignore[arg-type]

    def test_none_config_raises(self):
        with pytest.raises(SlicerError):
            slice_results([], None)  # type: ignore[arg-type]

    def test_empty_results_returns_empty_report(self):
        report = slice_results([], SliceConfig(fields=["image"]))
        assert len(report) == 0

    def test_keeps_only_matching_fields(self):
        result = _make("svc", ["image", "replicas", "memory"])
        report = slice_results([result], SliceConfig(fields=["image", "memory"]))
        assert len(report) == 1
        sliced = report.results[0]
        assert {d.field for d in sliced.kept} == {"image", "memory"}
        assert sliced.omitted_count == 1

    def test_no_matching_fields_gives_no_drift(self):
        result = _make("svc", ["replicas"])
        report = slice_results([result], SliceConfig(fields=["image"]))
        assert report.results[0].has_drift() is False
        assert report.results[0].omitted_count == 1

    def test_clean_result_stays_clean(self):
        result = DriftResult(service="clean", diffs=[])
        report = slice_results([result], SliceConfig(fields=["image"]))
        assert report.results[0].has_drift() is False
        assert report.results[0].omitted_count == 0

    def test_summary_counts_drifted(self):
        r1 = _make("a", ["image"])
        r2 = DriftResult(service="b", diffs=[])
        report = slice_results([r1, r2], SliceConfig(fields=["image"]))
        assert "1/2" in report.summary()

    def test_service_names_preserved(self):
        results = [_make("alpha", ["x"]), _make("beta", ["y"])]
        report = slice_results(results, SliceConfig(fields=["x"]))
        assert report.service_names() == ["alpha", "beta"]
