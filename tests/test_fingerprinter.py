"""Tests for driftwatch.fingerprinter."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.fingerprinter import (
    FingerprinterError,
    FingerprintedResult,
    fingerprint_results,
    _stable_fingerprint,
)


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=None, actual=None)


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# FingerprintedResult
# ---------------------------------------------------------------------------

class TestFingerprintedResult:
    def test_has_drift_false_when_empty(self):
        r = FingerprintedResult(service="svc", fingerprint="abc", drift_fields=[])
        assert r.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        r = FingerprintedResult(service="svc", fingerprint="abc", drift_fields=["port"])
        assert r.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        r = FingerprintedResult(service="svc", fingerprint="abc", drift_fields=["x"])
        d = r.to_dict()
        assert set(d.keys()) == {"service", "fingerprint", "drift_fields", "has_drift"}

    def test_to_dict_drift_fields_sorted(self):
        r = FingerprintedResult(service="svc", fingerprint="abc", drift_fields=["z", "a"])
        assert r.to_dict()["drift_fields"] == ["a", "z"]


# ---------------------------------------------------------------------------
# _stable_fingerprint
# ---------------------------------------------------------------------------

def test_fingerprint_is_hex_string():
    fp = _stable_fingerprint("svc", [])
    assert isinstance(fp, str)
    assert len(fp) == 64


def test_same_inputs_produce_same_fingerprint():
    diffs = [_diff("port"), _diff("replicas")]
    fp1 = _stable_fingerprint("auth", diffs)
    fp2 = _stable_fingerprint("auth", diffs)
    assert fp1 == fp2


def test_different_services_produce_different_fingerprints():
    diffs = [_diff("port")]
    assert _stable_fingerprint("svc-a", diffs) != _stable_fingerprint("svc-b", diffs)


def test_different_diffs_produce_different_fingerprints():
    assert _stable_fingerprint("svc", [_diff("port")]) != _stable_fingerprint("svc", [_diff("replicas")])


def test_diff_order_does_not_affect_fingerprint():
    d1, d2 = _diff("port"), _diff("replicas")
    fp1 = _stable_fingerprint("svc", [d1, d2])
    fp2 = _stable_fingerprint("svc", [d2, d1])
    assert fp1 == fp2


# ---------------------------------------------------------------------------
# fingerprint_results
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(FingerprinterError):
        fingerprint_results(None)  # type: ignore


def test_empty_list_returns_empty():
    assert fingerprint_results([]) == []


def test_single_clean_result():
    results = fingerprint_results([_make("svc")])
    assert len(results) == 1
    assert results[0].service == "svc"
    assert results[0].has_drift() is False


def test_drift_fields_captured():
    r = _make("svc", [_diff("port"), _diff("replicas")])
    out = fingerprint_results([r])
    assert set(out[0].drift_fields) == {"port", "replicas"}


def test_fingerprint_stable_across_calls():
    r = _make("svc", [_diff("port")])
    fp1 = fingerprint_results([r])[0].fingerprint
    fp2 = fingerprint_results([r])[0].fingerprint
    assert fp1 == fp2


def test_multiple_results_all_fingerprinted():
    results = [_make("a"), _make("b", [_diff("x")]), _make("c")]
    out = fingerprint_results(results)
    assert len(out) == 3
    assert [o.service for o in out] == ["a", "b", "c"]


def test_source_result_preserved():
    r = _make("svc")
    out = fingerprint_results([r])
    assert out[0].source_result is r
