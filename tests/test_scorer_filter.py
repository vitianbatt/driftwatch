"""Tests for driftwatch.scorer_filter."""
import pytest

from driftwatch.differ import FieldDiff
from driftwatch.comparator import DriftResult
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_filter import (
    ScorerFilterError,
    ScoreFilterConfig,
    FilteredScoredReport,
    filter_scored,
)


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=None, actual=None)


def _make(service: str, score: float, diffs=None) -> ScoredResult:
    drift = DriftResult(service=service, diffs=diffs or [])
    return ScoredResult(service=service, score=score, drift=drift)


def _report(*results: ScoredResult) -> ScoredReport:
    return ScoredReport(results=list(results))


# --- ScoreFilterConfig ---

class TestScoreFilterConfig:
    def test_defaults_are_valid(self):
        cfg = ScoreFilterConfig()
        assert cfg.min_score == 0.0
        assert cfg.max_score is None
        assert cfg.include_clean is True

    def test_negative_min_score_raises(self):
        with pytest.raises(ScorerFilterError, match="min_score"):
            ScoreFilterConfig(min_score=-1.0)

    def test_max_less_than_min_raises(self):
        with pytest.raises(ScorerFilterError, match="max_score"):
            ScoreFilterConfig(min_score=5.0, max_score=2.0)

    def test_equal_min_max_is_valid(self):
        cfg = ScoreFilterConfig(min_score=3.0, max_score=3.0)
        assert cfg.min_score == cfg.max_score


# --- filter_scored ---

class TestFilterScored:
    def test_none_report_raises(self):
        with pytest.raises(ScorerFilterError, match="report"):
            filter_scored(None, ScoreFilterConfig())

    def test_none_config_raises(self):
        with pytest.raises(ScorerFilterError, match="config"):
            filter_scored(_report(), None)

    def test_empty_report_returns_empty(self):
        result = filter_scored(_report(), ScoreFilterConfig())
        assert result.total_kept == 0
        assert result.total_input == 0
        assert result.total_excluded == 0

    def test_all_pass_with_defaults(self):
        r = _report(_make("a", 0.0), _make("b", 3.0), _make("c", 9.0))
        result = filter_scored(r, ScoreFilterConfig())
        assert result.total_kept == 3
        assert result.total_excluded == 0

    def test_min_score_filters_low(self):
        r = _report(_make("a", 1.0), _make("b", 5.0), _make("c", 2.5))
        result = filter_scored(r, ScoreFilterConfig(min_score=3.0))
        assert result.total_kept == 1
        assert result.results[0].service == "b"

    def test_max_score_filters_high(self):
        r = _report(_make("a", 1.0), _make("b", 5.0), _make("c", 9.0))
        result = filter_scored(r, ScoreFilterConfig(max_score=5.0))
        assert result.total_kept == 2
        services = {x.service for x in result.results}
        assert services == {"a", "b"}

    def test_exclude_clean_removes_zero_score(self):
        r = _report(_make("a", 0.0), _make("b", 4.0))
        result = filter_scored(r, ScoreFilterConfig(include_clean=False))
        assert result.total_kept == 1
        assert result.results[0].service == "b"
        assert result.total_excluded == 1

    def test_summary_string_format(self):
        r = _report(_make("a", 0.0), _make("b", 4.0), _make("c", 7.0))
        result = filter_scored(r, ScoreFilterConfig(min_score=3.0))
        s = result.summary()
        assert "kept=2" in s
        assert "excluded=1" in s
        assert "total=3" in s

    def test_total_kept_matches_results_len(self):
        r = _report(_make("x", 2.0), _make("y", 6.0))
        result = filter_scored(r, ScoreFilterConfig(min_score=5.0))
        assert result.total_kept == len(result.results)
