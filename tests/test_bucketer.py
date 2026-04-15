"""Tests for driftwatch.bucketer."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.bucketer import (
    Bucket,
    BucketedReport,
    BucketerError,
    bucket_results,
)


def _diff(field: str) -> FieldDiff:
    return FieldDiff(field=field, action="changed", expected="a", actual="b")


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# Bucket
# ---------------------------------------------------------------------------

class TestBucket:
    def test_len_empty(self):
        b = Bucket(name="alpha")
        assert len(b) == 0

    def test_len_with_results(self):
        b = Bucket(name="alpha", results=[_make("svc-a"), _make("svc-b")])
        assert len(b) == 2

    def test_service_names(self):
        b = Bucket(name="alpha", results=[_make("svc-a"), _make("svc-b")])
        assert b.service_names() == ["svc-a", "svc-b"]

    def test_drift_count_none_drifted(self):
        b = Bucket(name="alpha", results=[_make("svc-a"), _make("svc-b")])
        assert b.drift_count() == 0

    def test_drift_count_some_drifted(self):
        b = Bucket(
            name="alpha",
            results=[_make("svc-a", [_diff("env")]), _make("svc-b")],
        )
        assert b.drift_count() == 1

    def test_to_dict_keys(self):
        b = Bucket(name="alpha", results=[_make("svc-a")])
        d = b.to_dict()
        assert set(d.keys()) == {"name", "total", "drifted", "services"}

    def test_to_dict_values(self):
        b = Bucket(name="alpha", results=[_make("svc-a", [_diff("x")])])
        d = b.to_dict()
        assert d["name"] == "alpha"
        assert d["total"] == 1
        assert d["drifted"] == 1
        assert d["services"] == ["svc-a"]


# ---------------------------------------------------------------------------
# bucket_results
# ---------------------------------------------------------------------------

class TestBucketResults:
    def test_none_results_raises(self):
        with pytest.raises(BucketerError):
            bucket_results(None, {})

    def test_none_bucket_map_raises(self):
        with pytest.raises(BucketerError):
            bucket_results([], None)

    def test_empty_inputs_return_empty_report(self):
        report = bucket_results([], {})
        assert report.total() == 0
        assert report.bucket_names() == []

    def test_results_assigned_to_correct_bucket(self):
        results = [_make("auth"), _make("billing"), _make("gateway")]
        bucket_map = {
            "group-a": ["auth", "billing"],
            "group-b": ["gateway"],
        }
        report = bucket_results(results, bucket_map)
        assert len(report.get("group-a")) == 2
        assert len(report.get("group-b")) == 1

    def test_unmapped_service_not_in_any_bucket(self):
        results = [_make("unknown-svc")]
        bucket_map = {"group-a": ["auth"]}
        report = bucket_results(results, bucket_map)
        assert len(report.get("group-a")) == 0

    def test_total_sums_all_buckets(self):
        results = [_make("a"), _make("b"), _make("c")]
        bucket_map = {"x": ["a", "b"], "y": ["c"]}
        report = bucket_results(results, bucket_map)
        assert report.total() == 3

    def test_bucket_names_sorted(self):
        bucket_map = {"zebra": [], "apple": [], "mango": []}
        report = bucket_results([], bucket_map)
        assert report.bucket_names() == ["apple", "mango", "zebra"]

    def test_summary_no_buckets(self):
        report = BucketedReport()
        assert report.summary() == "No buckets."

    def test_summary_lists_buckets(self):
        results = [_make("auth", [_diff("replicas")]), _make("billing")]
        bucket_map = {"prod": ["auth", "billing"]}
        report = bucket_results(results, bucket_map)
        summary = report.summary()
        assert "prod" in summary
        assert "2 result(s)" in summary
        assert "1 drifted" in summary
