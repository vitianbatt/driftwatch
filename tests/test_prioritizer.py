"""Tests for driftwatch.prioritizer."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity
from driftwatch.prioritizer import (
    Priority,
    PrioritizerError,
    PrioritizedResult,
    prioritize,
    _compute_score,
    _score_to_priority,
)


def _make(service: str, missing=(), extra=(), changed=()) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=list(missing),
        extra_keys=list(extra),
        changed_keys=list(changed),
    )


# ---------------------------------------------------------------------------
# _score_to_priority
# ---------------------------------------------------------------------------

class TestScoreToPriority:
    def test_score_zero_is_low(self):
        assert _score_to_priority(0) == Priority.LOW

    def test_score_2_is_low(self):
        assert _score_to_priority(2) == Priority.LOW

    def test_score_3_is_normal(self):
        assert _score_to_priority(3) == Priority.NORMAL

    def test_score_6_is_high(self):
        assert _score_to_priority(6) == Priority.HIGH

    def test_score_15_is_critical(self):
        assert _score_to_priority(15) == Priority.CRITICAL

    def test_score_100_is_critical(self):
        assert _score_to_priority(100) == Priority.CRITICAL


# ---------------------------------------------------------------------------
# prioritize
# ---------------------------------------------------------------------------

class TestPrioritize:
    def test_none_raises(self):
        with pytest.raises(PrioritizerError):
            prioritize(None)

    def test_empty_list_returns_empty(self):
        assert prioritize([]) == []

    def test_clean_result_is_low_priority(self):
        results = [_make("svc-a")]
        out = prioritize(results)
        assert len(out) == 1
        assert out[0].priority == Priority.LOW
        assert out[0].severity == Severity.LOW

    def test_many_missing_keys_raises_priority(self):
        results = [_make("svc-b", missing=["a", "b", "c", "d"])]
        out = prioritize(results)
        assert out[0].priority in (Priority.HIGH, Priority.CRITICAL)

    def test_results_sorted_highest_first(self):
        low = _make("low-svc")
        high = _make("high-svc", missing=["x", "y", "z", "w", "v"])
        out = prioritize([low, high])
        assert out[0].result.service == "high-svc"
        assert out[-1].result.service == "low-svc"

    def test_to_dict_contains_expected_keys(self):
        results = [_make("svc-c", missing=["key1"])]
        out = prioritize(results)
        d = out[0].to_dict()
        assert "service" in d
        assert "priority" in d
        assert "severity" in d
        assert "score" in d
        assert "drift_fields" in d

    def test_drift_fields_combined_in_to_dict(self):
        results = [_make("svc-d", missing=["m"], extra=["e"], changed=["c"])]
        out = prioritize(results)
        d = out[0].to_dict()
        assert set(d["drift_fields"]) == {"m", "e", "c"}
