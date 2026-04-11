"""Tests for driftwatch.compactor."""
import pytest
from driftwatch.compactor import (
    CompactorError,
    CompactedResult,
    compact_results,
)
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _diff(field_name: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field_name, kind=kind, expected=None, actual=None)


def _make(service: str, *field_names: str) -> DriftResult:
    diffs = [_diff(f) for f in field_names]
    return DriftResult(service=service, drift_fields=diffs)


class TestCompactedResult:
    def test_has_drift_false_when_empty(self):
        r = CompactedResult(service="svc")
        assert not r.has_drift

    def test_has_drift_true_when_fields_present(self):
        r = CompactedResult(service="svc", drift_fields=[_diff("timeout")])
        assert r.has_drift

    def test_summary_clean(self):
        r = CompactedResult(service="svc", source_count=2)
        assert "clean" in r.summary()
        assert "2" in r.summary()

    def test_summary_with_drift(self):
        r = CompactedResult(service="svc", drift_fields=[_diff("port")], source_count=3)
        s = r.summary()
        assert "port" in s
        assert "3" in s

    def test_to_dict_keys(self):
        r = CompactedResult(service="svc", drift_fields=[_diff("x")], source_count=1)
        d = r.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "drift_fields", "source_count"}

    def test_to_dict_drift_fields_are_names(self):
        r = CompactedResult(service="svc", drift_fields=[_diff("alpha"), _diff("beta")])
        assert r.to_dict()["drift_fields"] == ["alpha", "beta"]


class TestCompactResults:
    def test_none_raises(self):
        with pytest.raises(CompactorError):
            compact_results(None)

    def test_empty_list_returns_empty(self):
        assert compact_results([]) == []

    def test_single_result_preserved(self):
        results = [_make("auth", "timeout")]
        out = compact_results(results)
        assert len(out) == 1
        assert out[0].service == "auth"
        assert out[0].source_count == 1

    def test_two_different_services_kept_separate(self):
        results = [_make("auth", "timeout"), _make("payments", "port")]
        out = compact_results(results)
        names = {r.service for r in out}
        assert names == {"auth", "payments"}

    def test_duplicate_services_merged(self):
        results = [_make("auth", "timeout"), _make("auth", "replicas")]
        out = compact_results(results)
        assert len(out) == 1
        assert out[0].source_count == 2
        fields = {d.field for d in out[0].drift_fields}
        assert fields == {"timeout", "replicas"}

    def test_duplicate_field_not_added_twice(self):
        results = [_make("auth", "timeout"), _make("auth", "timeout")]
        out = compact_results(results)
        assert len(out[0].drift_fields) == 1
        assert out[0].source_count == 2

    def test_clean_entries_merged_correctly(self):
        results = [_make("auth"), _make("auth")]
        out = compact_results(results)
        assert len(out) == 1
        assert not out[0].has_drift
        assert out[0].source_count == 2

    def test_order_follows_first_occurrence(self):
        results = [_make("alpha"), _make("beta"), _make("alpha")]
        out = compact_results(results)
        assert out[0].service == "alpha"
        assert out[1].service == "beta"
