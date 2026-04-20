"""Tests for driftwatch.scorer_threshold."""
import pytest
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_threshold import (
    ScorerThresholdError,
    ThresholdConfig,
    ThresholdedReport,
    apply_threshold,
)


def _diff(field: str = "replicas", kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="2", actual="3")


def _make(service: str, score: float, diffs=None) -> ScoredResult:
    dr = DriftResult(service=service, diffs=diffs or [])
    return ScoredResult(result=dr, score=score)


def _report(*results: ScoredResult) -> ScoredReport:
    return ScoredReport(results=list(results))


# --- ThresholdConfig ---

class TestThresholdConfig:
    def test_defaults_are_valid(self):
        cfg = ThresholdConfig()
        assert cfg.min_score == 0.0
        assert cfg.include_clean is False

    def test_custom_min_score_accepted(self):
        cfg = ThresholdConfig(min_score=5.0)
        assert cfg.min_score == 5.0

    def test_negative_min_score_raises(self):
        with pytest.raises(ScorerThresholdError, match="min_score"):
            ThresholdConfig(min_score=-1.0)

    def test_zero_min_score_accepted(self):
        cfg = ThresholdConfig(min_score=0.0)
        assert cfg.min_score == 0.0


# --- apply_threshold ---

class TestApplyThreshold:
    def test_none_report_raises(self):
        with pytest.raises(ScorerThresholdError):
            apply_threshold(None, ThresholdConfig())

    def test_none_config_raises(self):
        with pytest.raises(ScorerThresholdError):
            apply_threshold(_report(), None)

    def test_empty_report_returns_empty(self):
        result = apply_threshold(_report(), ThresholdConfig())
        assert result.total_kept == 0
        assert result.total_dropped == 0

    def test_clean_result_dropped_by_default(self):
        r = _make("svc-a", score=10.0)
        result = apply_threshold(_report(r), ThresholdConfig())
        assert result.total_kept == 0
        assert result.total_dropped == 1

    def test_clean_result_kept_when_include_clean(self):
        r = _make("svc-a", score=0.0)
        result = apply_threshold(_report(r), ThresholdConfig(include_clean=True))
        assert result.total_kept == 1

    def test_drifted_result_above_threshold_kept(self):
        r = _make("svc-b", score=7.0, diffs=[_diff()])
        result = apply_threshold(_report(r), ThresholdConfig(min_score=5.0))
        assert result.total_kept == 1

    def test_drifted_result_below_threshold_dropped(self):
        r = _make("svc-b", score=2.0, diffs=[_diff()])
        result = apply_threshold(_report(r), ThresholdConfig(min_score=5.0))
        assert result.total_dropped == 1

    def test_mixed_results_split_correctly(self):
        high = _make("svc-a", score=8.0, diffs=[_diff()])
        low = _make("svc-b", score=1.0, diffs=[_diff()])
        result = apply_threshold(_report(high, low), ThresholdConfig(min_score=5.0))
        assert result.total_kept == 1
        assert result.total_dropped == 1
        assert result.kept[0].result.service == "svc-a"

    def test_summary_contains_threshold(self):
        result = apply_threshold(_report(), ThresholdConfig(min_score=3.0))
        assert "3.0" in result.summary()
