"""Tests for driftwatch.weigher."""
from __future__ import annotations

import pytest

from driftwatch.weigher import (
    WeigherError,
    WeightMap,
    WeighedResult,
    weigh_diffs,
    total_weight,
)
from driftwatch.differ import FieldDiff


def _make(field: str = "replicas", kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="2", actual="3")


# ---------------------------------------------------------------------------
# TestWeightMap
# ---------------------------------------------------------------------------

class TestWeightMap:
    def test_valid_map_created(self):
        wm = WeightMap(weights={"replicas": 2.0, "image": 3.0})
        assert wm.get("replicas") == 2.0

    def test_missing_key_returns_default(self):
        wm = WeightMap(weights={}, default=1.0)
        assert wm.get("anything") == 1.0

    def test_custom_default_weight(self):
        wm = WeightMap(weights={}, default=5.0)
        assert wm.get("missing") == 5.0

    def test_empty_key_raises(self):
        with pytest.raises(WeigherError, match="empty"):
            WeightMap(weights={"": 1.0})

    def test_whitespace_key_raises(self):
        with pytest.raises(WeigherError, match="empty"):
            WeightMap(weights={"   ": 1.0})

    def test_negative_weight_raises(self):
        with pytest.raises(WeigherError, match="non-negative"):
            WeightMap(weights={"replicas": -1.0})

    def test_negative_default_raises(self):
        with pytest.raises(WeigherError, match="non-negative"):
            WeightMap(weights={}, default=-0.5)

    def test_zero_weight_is_valid(self):
        wm = WeightMap(weights={"replicas": 0.0})
        assert wm.get("replicas") == 0.0


# ---------------------------------------------------------------------------
# TestWeighDiffs
# ---------------------------------------------------------------------------

class TestWeighDiffs:
    def test_empty_diffs_returns_empty(self):
        wm = WeightMap(weights={})
        assert weigh_diffs("svc", [], wm) == []

    def test_single_diff_uses_mapped_weight(self):
        wm = WeightMap(weights={"replicas": 4.0})
        result = weigh_diffs("svc", [_make("replicas")], wm)
        assert len(result) == 1
        assert result[0].weight == 4.0

    def test_unmapped_field_uses_default(self):
        wm = WeightMap(weights={}, default=2.5)
        result = weigh_diffs("svc", [_make("image")], wm)
        assert result[0].weight == 2.5

    def test_service_name_propagated(self):
        wm = WeightMap(weights={})
        result = weigh_diffs("auth-service", [_make()], wm)
        assert result[0].service == "auth-service"

    def test_kind_propagated(self):
        wm = WeightMap(weights={})
        diff = FieldDiff(field="replicas", kind="missing", expected="2", actual=None)
        result = weigh_diffs("svc", [diff], wm)
        assert result[0].kind == "missing"

    def test_multiple_diffs_all_weighed(self):
        wm = WeightMap(weights={"replicas": 2.0, "image": 3.0})
        diffs = [_make("replicas"), _make("image"), _make("env")]
        result = weigh_diffs("svc", diffs, wm)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# TestTotalWeight
# ---------------------------------------------------------------------------

class TestTotalWeight:
    def test_empty_list_returns_zero(self):
        assert total_weight([]) == 0.0

    def test_sums_weights_correctly(self):
        results = [
            WeighedResult(service="s", field="a", weight=2.0, kind="changed"),
            WeighedResult(service="s", field="b", weight=3.5, kind="missing"),
        ]
        assert total_weight(results) == pytest.approx(5.5)

    def test_to_dict_contains_all_keys(self):
        wr = WeighedResult(service="svc", field="replicas", weight=1.5, kind="changed")
        d = wr.to_dict()
        assert set(d.keys()) == {"service", "field", "weight", "kind"}
