"""Tests for driftwatch.normalizer."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.normalizer import (
    NormalizationMap,
    NormalizerError,
    NormalizedResult,
    normalize_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, fields=None) -> DriftResult:
    diffs = [
        FieldDiff(field=f, kind="changed", expected="a", actual="b")
        for f in (fields or [])
    ]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# NormalizationMap
# ---------------------------------------------------------------------------

class TestNormalizationMap:
    def test_valid_map_created(self):
        nm = NormalizationMap(rules={"mem_limit": "memory_limit"})
        assert nm.translate("mem_limit") == "memory_limit"

    def test_unmapped_key_returned_unchanged(self):
        nm = NormalizationMap(rules={"mem_limit": "memory_limit"})
        assert nm.translate("cpu_limit") == "cpu_limit"

    def test_empty_raw_key_raises(self):
        with pytest.raises(NormalizerError):
            NormalizationMap(rules={"": "memory_limit"})

    def test_whitespace_raw_key_raises(self):
        with pytest.raises(NormalizerError):
            NormalizationMap(rules={"   ": "memory_limit"})

    def test_empty_canonical_raises(self):
        with pytest.raises(NormalizerError):
            NormalizationMap(rules={"mem_limit": ""})

    def test_non_dict_rules_raises(self):
        with pytest.raises(NormalizerError):
            NormalizationMap(rules=["mem_limit"])


# ---------------------------------------------------------------------------
# NormalizedResult
# ---------------------------------------------------------------------------

class TestNormalizedResult:
    def test_has_drift_false_when_empty(self):
        original = _make("svc-a")
        nr = NormalizedResult(service="svc-a", diffs=[], original=original)
        assert not nr.has_drift()

    def test_has_drift_true_when_diffs(self):
        original = _make("svc-a", ["mem_limit"])
        nr = NormalizedResult(service="svc-a", diffs=original.diffs, original=original)
        assert nr.has_drift()

    def test_to_dict_contains_expected_keys(self):
        original = _make("svc-a", ["mem_limit"])
        nr = NormalizedResult(service="svc-a", diffs=original.diffs, original=original)
        d = nr.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "diff_count"}
        assert d["diff_count"] == 1


# ---------------------------------------------------------------------------
# normalize_results
# ---------------------------------------------------------------------------

class TestNormalizeResults:
    def test_empty_list_returns_empty(self):
        nm = NormalizationMap(rules={"mem_limit": "memory_limit"})
        assert normalize_results([], nm) == []

    def test_none_results_raises(self):
        nm = NormalizationMap(rules={})
        with pytest.raises(NormalizerError):
            normalize_results(None, nm)

    def test_none_map_raises(self):
        with pytest.raises(NormalizerError):
            normalize_results([], None)

    def test_field_key_translated(self):
        nm = NormalizationMap(rules={"mem_limit": "memory_limit"})
        result = _make("svc-a", ["mem_limit"])
        out = normalize_results([result], nm)
        assert out[0].diffs[0].field == "memory_limit"

    def test_unmapped_field_preserved(self):
        nm = NormalizationMap(rules={"mem_limit": "memory_limit"})
        result = _make("svc-a", ["cpu_limit"])
        out = normalize_results([result], nm)
        assert out[0].diffs[0].field == "cpu_limit"

    def test_original_result_stored(self):
        nm = NormalizationMap(rules={"mem_limit": "memory_limit"})
        result = _make("svc-a", ["mem_limit"])
        out = normalize_results([result], nm)
        assert out[0].original is result

    def test_multiple_results_all_translated(self):
        nm = NormalizationMap(rules={"mem": "memory", "cpu": "cpu_limit"})
        results = [_make("a", ["mem"]), _make("b", ["cpu"]), _make("c")]
        out = normalize_results(results, nm)
        assert out[0].diffs[0].field == "memory"
        assert out[1].diffs[0].field == "cpu_limit"
        assert out[2].diffs == []
