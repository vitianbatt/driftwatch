"""Tests for driftwatch.leveler."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.leveler import (
    DriftLevel,
    LevelConfig,
    LeveledResult,
    LevelerError,
    level_results,
)


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


# ---------------------------------------------------------------------------
# LevelConfig validation
# ---------------------------------------------------------------------------

class TestLevelConfig:
    def test_defaults_are_valid(self):
        cfg = LevelConfig()
        assert cfg.low_threshold == 1
        assert cfg.medium_threshold == 3
        assert cfg.high_threshold == 6

    def test_custom_thresholds_valid(self):
        cfg = LevelConfig(low_threshold=2, medium_threshold=5, high_threshold=10)
        assert cfg.high_threshold == 10

    def test_zero_low_raises(self):
        with pytest.raises(LevelerError):
            LevelConfig(low_threshold=0)

    def test_inverted_thresholds_raise(self):
        with pytest.raises(LevelerError):
            LevelConfig(low_threshold=5, medium_threshold=3, high_threshold=10)


# ---------------------------------------------------------------------------
# LeveledResult helpers
# ---------------------------------------------------------------------------

class TestLeveledResult:
    def test_has_drift_false_when_none(self):
        r = LeveledResult(service="svc", level=DriftLevel.NONE, drift_field_count=0)
        assert not r.has_drift()

    def test_has_drift_true_when_low(self):
        r = LeveledResult(service="svc", level=DriftLevel.LOW, drift_field_count=1, drifted_fields=["x"])
        assert r.has_drift()

    def test_to_dict_contains_all_keys(self):
        r = LeveledResult(service="svc", level=DriftLevel.HIGH, drift_field_count=7, drifted_fields=["a", "b"])
        d = r.to_dict()
        assert set(d.keys()) == {"service", "level", "drift_field_count", "drifted_fields"}

    def test_to_dict_level_is_string(self):
        r = LeveledResult(service="svc", level=DriftLevel.MEDIUM, drift_field_count=4)
        assert r.to_dict()["level"] == "medium"


# ---------------------------------------------------------------------------
# level_results
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(LevelerError):
        level_results(None)


def test_empty_list_returns_empty():
    assert level_results([]) == []


def test_no_drift_fields_is_none_level():
    results = [_make("auth")]
    out = level_results(results)
    assert out[0].level == DriftLevel.NONE


def test_one_field_is_low():
    results = [_make("auth", ["replicas"])]
    out = level_results(results)
    assert out[0].level == DriftLevel.LOW


def test_three_fields_is_medium():
    results = [_make("auth", ["a", "b", "c"])]
    out = level_results(results)
    assert out[0].level == DriftLevel.MEDIUM


def test_six_fields_is_high():
    results = [_make("svc", ["a", "b", "c", "d", "e", "f"])]
    out = level_results(results)
    assert out[0].level == DriftLevel.HIGH


def test_seven_fields_is_critical():
    results = [_make("svc", ["a", "b", "c", "d", "e", "f", "g"])]
    out = level_results(results)
    assert out[0].level == DriftLevel.CRITICAL


def test_multiple_results_leveled_independently():
    results = [_make("a"), _make("b", ["x"]), _make("c", ["x", "y", "z", "w", "v", "u", "t"])]
    out = level_results(results)
    assert out[0].level == DriftLevel.NONE
    assert out[1].level == DriftLevel.LOW
    assert out[2].level == DriftLevel.CRITICAL


def test_custom_config_changes_boundaries():
    cfg = LevelConfig(low_threshold=1, medium_threshold=2, high_threshold=3)
    results = [_make("svc", ["a", "b", "c", "d"])]
    out = level_results(results, config=cfg)
    assert out[0].level == DriftLevel.CRITICAL


def test_service_name_preserved():
    results = [_make("payment-service", ["timeout"])]
    out = level_results(results)
    assert out[0].service == "payment-service"
