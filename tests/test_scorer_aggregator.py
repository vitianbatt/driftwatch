"""Tests for driftwatch.scorer_aggregator."""
import pytest
from driftwatch.scorer import ScoredResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer_aggregator import (
    AggregatedScoredReport,
    ScorerAggregatorError,
    aggregate_scored,
)


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="x", actual=None)


def _make(service: str, score: float, diffs=None) -> ScoredResult:
    return ScoredResult(
        service=service,
        score=score,
        diffs=diffs or [],
    )


def test_none_raises():
    with pytest.raises(ScorerAggregatorError):
        aggregate_scored(None)


def test_empty_list_returns_zero_counts():
    report = aggregate_scored([])
    assert report.total == 0
    assert report.drifted == 0
    assert report.clean == 0
    assert report.min_score == 0.0
    assert report.max_score == 0.0
    assert report.mean_score == 0.0


def test_all_clean_services():
    results = [_make("svc-a", 0.0), _make("svc-b", 0.0)]
    report = aggregate_scored(results)
    assert report.total == 2
    assert report.drifted == 0
    assert report.clean == 2


def test_all_drifted_services():
    results = [_make("svc-a", 3.0), _make("svc-b", 7.0)]
    report = aggregate_scored(results)
    assert report.drifted == 2
    assert report.clean == 0


def test_min_max_mean_score():
    results = [_make("a", 2.0), _make("b", 4.0), _make("c", 6.0)]
    report = aggregate_scored(results)
    assert report.min_score == 2.0
    assert report.max_score == 6.0
    assert report.mean_score == pytest.approx(4.0)


def test_drift_rate_partial():
    results = [_make("a", 0.0), _make("b", 5.0)]
    report = aggregate_scored(results)
    assert report.drift_rate() == pytest.approx(0.5)


def test_drift_rate_empty():
    report = aggregate_scored([])
    assert report.drift_rate() == 0.0


def test_top_returns_highest_scored():
    results = [_make("a", 1.0), _make("b", 9.0), _make("c", 4.0)]
    report = aggregate_scored(results)
    top = report.top(2)
    assert len(top) == 2
    assert top[0].service == "b"
    assert top[1].service == "c"


def test_top_n_larger_than_results():
    results = [_make("a", 1.0)]
    report = aggregate_scored(results)
    assert len(report.top(10)) == 1


def test_to_dict_contains_all_keys():
    results = [_make("a", 2.0), _make("b", 0.0)]
    report = aggregate_scored(results)
    d = report.to_dict()
    for key in ("total", "drifted", "clean", "min_score", "max_score", "mean_score", "drift_rate"):
        assert key in d


def test_to_dict_values_correct():
    results = [_make("a", 4.0), _make("b", 0.0)]
    report = aggregate_scored(results)
    d = report.to_dict()
    assert d["total"] == 2
    assert d["drifted"] == 1
    assert d["drift_rate"] == pytest.approx(0.5)
