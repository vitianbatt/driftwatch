"""Tests for driftwatch.indexer."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.indexer import FieldIndex, IndexerError, build_index


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _diff(fname: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=fname, kind=kind, expected="a", actual=None)


def _make(service: str, *field_names: str) -> DriftResult:
    diffs = [_diff(f) for f in field_names]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# build_index
# ---------------------------------------------------------------------------

class TestBuildIndex:
    def test_none_raises(self):
        with pytest.raises(IndexerError):
            build_index(None)  # type: ignore[arg-type]

    def test_empty_list_returns_empty_index(self):
        idx = build_index([])
        assert idx.total_fields() == 0
        assert idx.total_entries() == 0

    def test_single_result_no_drift(self):
        result = _make("auth")  # no diffs
        idx = build_index([result])
        assert idx.total_fields() == 0

    def test_single_field_single_service(self):
        idx = build_index([_make("auth", "timeout")])
        assert idx.total_fields() == 1
        assert idx.services_for("timeout") == ["auth"]

    def test_same_field_multiple_services(self):
        results = [
            _make("auth", "timeout"),
            _make("billing", "timeout"),
        ]
        idx = build_index(results)
        assert idx.total_fields() == 1
        assert idx.services_for("timeout") == ["auth", "billing"]

    def test_multiple_fields(self):
        results = [
            _make("auth", "timeout", "retries"),
            _make("billing", "retries"),
        ]
        idx = build_index(results)
        assert idx.total_fields() == 2
        assert set(idx.field_names()) == {"timeout", "retries"}

    def test_duplicate_service_not_repeated(self):
        # same service appears twice with same field
        results = [
            _make("auth", "timeout"),
            _make("auth", "timeout"),
        ]
        idx = build_index(results)
        assert idx.services_for("timeout") == ["auth"]

    def test_total_entries_counts_pairs(self):
        results = [
            _make("auth", "timeout", "retries"),
            _make("billing", "timeout"),
        ]
        idx = build_index(results)
        # timeout -> [auth, billing], retries -> [auth]  => 3 pairs
        assert idx.total_entries() == 3


# ---------------------------------------------------------------------------
# FieldIndex helpers
# ---------------------------------------------------------------------------

class TestFieldIndex:
    def test_field_names_sorted(self):
        idx = FieldIndex(index={"zzz": ["svc"], "aaa": ["svc"]})
        assert idx.field_names() == ["aaa", "zzz"]

    def test_services_for_missing_field_returns_empty(self):
        idx = FieldIndex(index={})
        assert idx.services_for("nonexistent") == []

    def test_summary_empty(self):
        idx = FieldIndex(index={})
        assert idx.summary() == "index is empty"

    def test_summary_non_empty(self):
        idx = FieldIndex(index={"timeout": ["auth", "billing"]})
        summary = idx.summary()
        assert "timeout" in summary
        assert "auth" in summary
        assert "billing" in summary
