"""Tests for driftwatch.capper."""
from __future__ import annotations

import pytest

from driftwatch.capper import (
    CapConfig,
    CappedReport,
    CappedResult,
    CapperError,
    cap_results,
)
from driftwatch.comparator import DriftResult


def _make(service: str, fields: list[str]) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields)


# ---------------------------------------------------------------------------
# CapConfig
# ---------------------------------------------------------------------------

class TestCapConfig:
    def test_default_max_diffs(self):
        cfg = CapConfig()
        assert cfg.max_diffs == 5

    def test_custom_max_diffs(self):
        cfg = CapConfig(max_diffs=3)
        assert cfg.max_diffs == 3

    def test_zero_max_diffs_raises(self):
        with pytest.raises(CapperError):
            CapConfig(max_diffs=0)

    def test_negative_max_diffs_raises(self):
        with pytest.raises(CapperError):
            CapConfig(max_diffs=-1)


# ---------------------------------------------------------------------------
# CappedResult
# ---------------------------------------------------------------------------

class TestCappedResult:
    def test_has_drift_false_when_empty(self):
        r = CappedResult(service="svc", drifted_fields=[], was_capped=False, original_count=0)
        assert r.has_drift() is False

    def test_has_drift_true_when_fields(self):
        r = CappedResult(service="svc", drifted_fields=["x"], was_capped=False, original_count=1)
        assert r.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        r = CappedResult(service="svc", drifted_fields=["a"], was_capped=True, original_count=9)
        d = r.to_dict()
        assert set(d.keys()) == {"service", "drifted_fields", "was_capped", "original_count"}

    def test_to_dict_values(self):
        r = CappedResult(service="auth", drifted_fields=["a", "b"], was_capped=True, original_count=7)
        d = r.to_dict()
        assert d["service"] == "auth"
        assert d["drifted_fields"] == ["a", "b"]
        assert d["was_capped"] is True
        assert d["original_count"] == 7


# ---------------------------------------------------------------------------
# cap_results
# ---------------------------------------------------------------------------

class TestCapResults:
    def test_none_raises(self):
        with pytest.raises(CapperError):
            cap_results(None)

    def test_empty_list_returns_empty_report(self):
        report = cap_results([])
        assert isinstance(report, CappedReport)
        assert report.results == []

    def test_under_limit_not_capped(self):
        r = _make("svc", ["a", "b"])
        report = cap_results([r], CapConfig(max_diffs=5))
        assert len(report.results) == 1
        assert report.results[0].was_capped is False
        assert report.results[0].original_count == 2

    def test_over_limit_is_capped(self):
        r = _make("svc", ["a", "b", "c", "d", "e", "f"])
        report = cap_results([r], CapConfig(max_diffs=3))
        res = report.results[0]
        assert res.was_capped is True
        assert len(res.drifted_fields) == 3
        assert res.original_count == 6

    def test_exactly_at_limit_not_capped(self):
        r = _make("svc", ["a", "b", "c"])
        report = cap_results([r], CapConfig(max_diffs=3))
        assert report.results[0].was_capped is False

    def test_total_capped_counts_correctly(self):
        results = [
            _make("svc-a", ["x", "y", "z"]),
            _make("svc-b", ["x"]),
        ]
        report = cap_results(results, CapConfig(max_diffs=2))
        assert report.total_capped() == 1

    def test_summary_no_capped(self):
        report = cap_results([_make("svc", ["a"])], CapConfig(max_diffs=5))
        assert "0 capped" in report.summary()

    def test_summary_with_capped(self):
        report = cap_results([_make("svc", ["a", "b", "c"])], CapConfig(max_diffs=1))
        assert "1 capped" in report.summary()

    def test_summary_empty(self):
        report = cap_results([])
        assert "No results" in report.summary()
