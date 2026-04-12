"""Tests for driftwatch.clamper."""
import pytest
from driftwatch.clamper import ClampConfig, ClampedResult, ClamperError, clamp_results
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="x", actual=None)


def _make(service: str, fields=()) -> DriftResult:
    return DriftResult(service=service, drifted_fields=list(fields))


# --- ClampConfig ---

class TestClampConfig:
    def test_default_max_diffs(self):
        cfg = ClampConfig()
        assert cfg.max_diffs == 5

    def test_custom_max_diffs(self):
        cfg = ClampConfig(max_diffs=2)
        assert cfg.max_diffs == 2

    def test_zero_max_diffs_raises(self):
        with pytest.raises(ClamperError, match="max_diffs"):
            ClampConfig(max_diffs=0)

    def test_negative_max_diffs_raises(self):
        with pytest.raises(ClamperError, match="max_diffs"):
            ClampConfig(max_diffs=-1)

    def test_empty_marker_raises(self):
        with pytest.raises(ClamperError, match="truncation_marker"):
            ClampConfig(truncation_marker="")


# --- clamp_results ---

class TestClampResults:
    def test_none_raises(self):
        with pytest.raises(ClamperError):
            clamp_results(None)

    def test_empty_list_returns_empty(self):
        assert clamp_results([]) == []

    def test_clean_result_not_truncated(self):
        r = _make("svc-a")
        result = clamp_results([r])
        assert len(result) == 1
        assert not result[0].truncated
        assert result[0].original_count == 0

    def test_within_limit_not_truncated(self):
        r = _make("svc-b", [_diff("f1"), _diff("f2")])
        result = clamp_results([r], ClampConfig(max_diffs=5))
        assert not result[0].truncated
        assert len(result[0].drifted_fields) == 2

    def test_exceeds_limit_truncated(self):
        diffs = [_diff(f"f{i}") for i in range(7)]
        r = _make("svc-c", diffs)
        result = clamp_results([r], ClampConfig(max_diffs=3))
        assert result[0].truncated
        assert len(result[0].drifted_fields) == 3
        assert result[0].original_count == 7

    def test_exactly_at_limit_not_truncated(self):
        diffs = [_diff(f"f{i}") for i in range(4)]
        r = _make("svc-d", diffs)
        result = clamp_results([r], ClampConfig(max_diffs=4))
        assert not result[0].truncated

    def test_multiple_results_each_clamped_independently(self):
        r1 = _make("svc-e", [_diff("a"), _diff("b"), _diff("c")])
        r2 = _make("svc-f", [_diff("x")])
        results = clamp_results([r1, r2], ClampConfig(max_diffs=2))
        assert results[0].truncated
        assert not results[1].truncated


# --- ClampedResult helpers ---

class TestClampedResult:
    def test_has_drift_false_when_empty(self):
        cr = ClampedResult(service="s", drifted_fields=[], truncated=False, original_count=0)
        assert not cr.has_drift()

    def test_has_drift_true_when_fields(self):
        cr = ClampedResult(service="s", drifted_fields=[_diff("x")], truncated=False, original_count=1)
        assert cr.has_drift()

    def test_summary_clean(self):
        cr = ClampedResult(service="svc", drifted_fields=[], truncated=False, original_count=0)
        assert "clean" in cr.summary()

    def test_summary_with_truncation_note(self):
        cr = ClampedResult(
            service="svc",
            drifted_fields=[_diff("a"), _diff("b")],
            truncated=True,
            original_count=5,
        )
        assert "+3 truncated" in cr.summary()

    def test_to_dict_keys(self):
        cr = ClampedResult(service="s", drifted_fields=[], truncated=False, original_count=0)
        d = cr.to_dict()
        assert set(d.keys()) == {"service", "drifted_fields", "truncated", "original_count"}
