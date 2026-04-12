"""Tests for driftwatch.weigher."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.weigher import (
    WeighedReport,
    WeigherError,
    WeightMap,
    weigh_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, diffs: dict | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or {})


# ---------------------------------------------------------------------------
# TestWeightMap
# ---------------------------------------------------------------------------

class TestWeightMap:
    def test_valid_map_created(self):
        wm = WeightMap(weights={"replicas": 2.0, "image": 3.0})
        assert wm.get("replicas") == 2.0
        assert wm.get("image") == 3.0

    def test_missing_key_returns_default(self):
        wm = WeightMap(weights={"replicas": 2.0}, default_weight=1.0)
        assert wm.get("unknown_field") == 1.0

    def test_custom_default_weight(self):
        wm = WeightMap(weights={}, default_weight=5.0)
        assert wm.get("anything") == 5.0

    def test_zero_default_raises(self):
        with pytest.raises(WeigherError, match="default_weight"):
            WeightMap(weights={}, default_weight=0)

    def test_negative_default_raises(self):
        with pytest.raises(WeigherError, match="default_weight"):
            WeightMap(weights={}, default_weight=-1.0)

    def test_zero_field_weight_raises(self):
        with pytest.raises(WeigherError, match="replicas"):
            WeightMap(weights={"replicas": 0})

    def test_negative_field_weight_raises(self):
        with pytest.raises(WeigherError, match="image"):
            WeightMap(weights={"image": -3.0})

    def test_empty_field_name_raises(self):
        with pytest.raises(WeigherError, match="empty"):
            WeightMap(weights={"": 1.0})

    def test_whitespace_field_name_raises(self):
        with pytest.raises(WeigherError, match="empty"):
            WeightMap(weights={"   ": 1.0})


# ---------------------------------------------------------------------------
# TestWeighResults
# ---------------------------------------------------------------------------

class TestWeighResults:
    def test_none_raises(self):
        wm = WeightMap(weights={})
        with pytest.raises(WeigherError):
            weigh_results(None, wm)

    def test_empty_list_returns_empty_report(self):
        wm = WeightMap(weights={})
        report = weigh_results([], wm)
        assert isinstance(report, WeighedReport)
        assert report.results == []
        assert report.total_score() == 0.0

    def test_clean_result_scores_zero(self):
        wm = WeightMap(weights={"replicas": 3.0})
        report = weigh_results([_make("svc-a")], wm)
        assert report.results[0].score == 0.0

    def test_single_drifted_field_uses_weight(self):
        wm = WeightMap(weights={"replicas": 4.0})
        r = _make("svc-b", {"replicas": {"expected": 3, "actual": 1}})
        report = weigh_results([r], wm)
        assert report.results[0].score == 4.0

    def test_multiple_fields_sum_weights(self):
        wm = WeightMap(weights={"replicas": 2.0, "image": 3.0})
        r = _make("svc-c", {
            "replicas": {"expected": 2, "actual": 1},
            "image": {"expected": "v1", "actual": "v2"},
        })
        report = weigh_results([r], wm)
        assert report.results[0].score == 5.0

    def test_unknown_field_uses_default_weight(self):
        wm = WeightMap(weights={}, default_weight=1.5)
        r = _make("svc-d", {"memory": {"expected": "512Mi", "actual": "256Mi"}})
        report = weigh_results([r], wm)
        assert report.results[0].score == 1.5

    def test_total_score_sums_all_results(self):
        wm = WeightMap(weights={"replicas": 2.0}, default_weight=1.0)
        results = [
            _make("svc-a", {"replicas": {}}),
            _make("svc-b", {"replicas": {}}),
        ]
        report = weigh_results(results, wm)
        assert report.total_score() == 4.0

    def test_top_returns_highest_scored_first(self):
        wm = WeightMap(weights={"replicas": 5.0, "image": 1.0})
        results = [
            _make("low", {"image": {}}),
            _make("high", {"replicas": {}}),
        ]
        report = weigh_results(results, wm)
        top = report.top(1)
        assert top[0].service == "high"

    def test_drifted_fields_recorded(self):
        wm = WeightMap(weights={"env": 2.0})
        r = _make("svc-e", {"env": {"expected": "prod", "actual": "staging"}})
        report = weigh_results([r], wm)
        assert "env" in report.results[0].drifted_fields
