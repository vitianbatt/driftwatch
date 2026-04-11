"""Tests for driftwatch.digester."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.digester import (
    DigestedResult,
    DigesterError,
    compute_digest,
    digest_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _diff(field: str, expected: str = "a", actual: str = "b") -> FieldDiff:
    return FieldDiff(field=field, expected=expected, actual=actual)


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# DigestedResult
# ---------------------------------------------------------------------------

class TestDigestedResult:
    def test_has_changed_false_when_no_previous(self):
        r = DigestedResult(service="svc", digest="abc123")
        assert r.has_changed is False

    def test_has_changed_false_when_digests_match(self):
        r = DigestedResult(service="svc", digest="abc", previous_digest="abc")
        assert r.has_changed is False

    def test_has_changed_true_when_digests_differ(self):
        r = DigestedResult(service="svc", digest="new", previous_digest="old")
        assert r.has_changed is True

    def test_to_dict_contains_all_keys(self):
        r = DigestedResult(service="svc", digest="d", drift_fields=["x"])
        d = r.to_dict()
        assert set(d) == {"service", "digest", "drift_fields", "previous_digest", "has_changed"}

    def test_to_dict_values(self):
        r = DigestedResult(service="svc", digest="d", drift_fields=["x"], previous_digest="old")
        d = r.to_dict()
        assert d["service"] == "svc"
        assert d["drift_fields"] == ["x"]
        assert d["has_changed"] is True


# ---------------------------------------------------------------------------
# compute_digest
# ---------------------------------------------------------------------------

class TestComputeDigest:
    def test_returns_string(self):
        assert isinstance(compute_digest({"a": 1}), str)

    def test_deterministic(self):
        assert compute_digest({"a": 1}) == compute_digest({"a": 1})

    def test_different_data_different_digest(self):
        assert compute_digest({"a": 1}) != compute_digest({"a": 2})

    def test_key_order_invariant(self):
        assert compute_digest({"a": 1, "b": 2}) == compute_digest({"b": 2, "a": 1})

    def test_unknown_algorithm_raises(self):
        with pytest.raises(DigesterError, match="Unknown hash algorithm"):
            compute_digest({"x": 1}, algorithm="notreal")

    def test_md5_algorithm_accepted(self):
        result = compute_digest({"x": 1}, algorithm="md5")
        assert len(result) == 32


# ---------------------------------------------------------------------------
# digest_results
# ---------------------------------------------------------------------------

class TestDigestResults:
    def test_empty_list_returns_empty(self):
        assert digest_results([]) == []

    def test_none_raises(self):
        with pytest.raises(DigesterError):
            digest_results(None)  # type: ignore[arg-type]

    def test_single_clean_result(self):
        out = digest_results([_make("auth")])
        assert len(out) == 1
        assert out[0].service == "auth"
        assert out[0].drift_fields == []

    def test_drift_fields_captured(self):
        out = digest_results([_make("svc", [_diff("timeout"), _diff("replicas")])])
        assert out[0].drift_fields == ["timeout", "replicas"]

    def test_previous_digest_none_by_default(self):
        out = digest_results([_make("svc")])
        assert out[0].previous_digest is None

    def test_previous_digest_attached(self):
        out = digest_results([_make("svc")], previous={"svc": "oldhash"})
        assert out[0].previous_digest == "oldhash"

    def test_has_changed_true_when_config_differs(self):
        result = _make("svc", [_diff("port")])
        out = digest_results([result], previous={"svc": "oldhash"})
        assert out[0].has_changed is True

    def test_order_preserved(self):
        results = [_make("alpha"), _make("beta"), _make("gamma")]
        out = digest_results(results)
        assert [r.service for r in out] == ["alpha", "beta", "gamma"]
