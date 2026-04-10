"""Tests for driftwatch.sampler."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.sampler import SampleReport, SamplerError, sample_results


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field: str) -> FieldDiff:
    return FieldDiff(field=field, expected="a", actual="b", kind="changed")


# ---------------------------------------------------------------------------
# TestSampleReport
# ---------------------------------------------------------------------------

class TestSampleReport:
    def test_len_reflects_sampled(self):
        r = SampleReport(sampled=[_make("svc-a")], total_input=5, seed=None)
        assert len(r) == 1

    def test_service_names_order_preserved(self):
        results = [_make("alpha"), _make("beta")]
        r = SampleReport(sampled=results, total_input=10, seed=42)
        assert r.service_names() == ["alpha", "beta"]

    def test_summary_no_results(self):
        r = SampleReport(sampled=[], total_input=0, seed=None)
        assert r.summary() == "No results sampled."

    def test_summary_with_drift(self):
        results = [_make("a", [_diff("x")]), _make("b")]
        r = SampleReport(sampled=results, total_input=10, seed=1)
        assert "1 with drift" in r.summary()
        assert "2 of 10" in r.summary()


# ---------------------------------------------------------------------------
# TestSampleResults
# ---------------------------------------------------------------------------

class TestSampleResults:
    def test_none_results_raises(self):
        with pytest.raises(SamplerError, match="None"):
            sample_results(None, n=2)

    def test_zero_n_raises(self):
        with pytest.raises(SamplerError, match=">= 1"):
            sample_results([_make("a")], n=0)

    def test_negative_n_raises(self):
        with pytest.raises(SamplerError, match=">= 1"):
            sample_results([_make("a")], n=-3)

    def test_empty_input_returns_empty(self):
        report = sample_results([], n=5)
        assert len(report) == 0
        assert report.total_input == 0

    def test_n_larger_than_input_returns_all(self):
        data = [_make(f"svc-{i}") for i in range(3)]
        report = sample_results(data, n=100)
        assert len(report) == 3

    def test_exact_n_returned(self):
        data = [_make(f"svc-{i}") for i in range(10)]
        report = sample_results(data, n=4, seed=0)
        assert len(report) == 4

    def test_seed_produces_same_sample(self):
        data = [_make(f"svc-{i}") for i in range(20)]
        r1 = sample_results(data, n=5, seed=99)
        r2 = sample_results(data, n=5, seed=99)
        assert r1.service_names() == r2.service_names()

    def test_different_seeds_likely_differ(self):
        data = [_make(f"svc-{i}") for i in range(20)]
        r1 = sample_results(data, n=5, seed=1)
        r2 = sample_results(data, n=5, seed=2)
        # Not guaranteed but overwhelmingly likely with 20 items
        assert r1.service_names() != r2.service_names()

    def test_seed_stored_in_report(self):
        data = [_make("a"), _make("b")]
        report = sample_results(data, n=1, seed=7)
        assert report.seed == 7

    def test_no_seed_stored_as_none(self):
        data = [_make("a")]
        report = sample_results(data, n=1)
        assert report.seed is None
