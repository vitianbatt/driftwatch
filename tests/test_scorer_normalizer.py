"""Tests for driftwatch.scorer_normalizer."""

from __future__ import annotations

import pytest

from driftwatch.scorer import ScoredResult
from driftwatch.scorer_normalizer import (
    ScorerNormalizerError,
    NormalizedScoredReport,
    normalize_scores,
)
from driftwatch.differ import FieldDiff


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=None, actual=None)


def _make(service: str, score: float, fields=()) -> ScoredResult:
    diffs = [_diff(f) for f in fields]
    return ScoredResult(
        service=service,
        score=score,
        drifted_fields=list(fields),
        diffs=diffs,
    )


# ---------------------------------------------------------------------------
# None / empty guards
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(ScorerNormalizerError):
        normalize_scores(None)


def test_empty_list_returns_empty_report():
    report = normalize_scores([])
    assert isinstance(report, NormalizedScoredReport)
    assert report.results == []
    assert report.min_raw == 0.0
    assert report.max_raw == 0.0


def test_invalid_floor_ceiling_raises():
    with pytest.raises(ScorerNormalizerError):
        normalize_scores([_make("svc", 1.0)], floor=1.0, ceiling=0.5)


def test_equal_floor_ceiling_raises():
    with pytest.raises(ScorerNormalizerError):
        normalize_scores([_make("svc", 1.0)], floor=0.5, ceiling=0.5)


# ---------------------------------------------------------------------------
# Normalization correctness
# ---------------------------------------------------------------------------

def test_single_result_gets_floor():
    report = normalize_scores([_make("svc", 5.0)])
    assert report.results[0].normalized_score == pytest.approx(0.0)


def test_two_results_min_is_floor_max_is_ceiling():
    results = [_make("low", 0.0), _make("high", 10.0)]
    report = normalize_scores(results)
    by_service = {r.service: r for r in report.results}
    assert by_service["low"].normalized_score == pytest.approx(0.0)
    assert by_service["high"].normalized_score == pytest.approx(1.0)


def test_midpoint_result_is_normalized_correctly():
    results = [_make("a", 0.0), _make("b", 5.0), _make("c", 10.0)]
    report = normalize_scores(results)
    by_service = {r.service: r for r in report.results}
    assert by_service["b"].normalized_score == pytest.approx(0.5)


def test_custom_floor_and_ceiling():
    results = [_make("a", 0.0), _make("b", 100.0)]
    report = normalize_scores(results, floor=10.0, ceiling=20.0)
    by_service = {r.service: r for r in report.results}
    assert by_service["a"].normalized_score == pytest.approx(10.0)
    assert by_service["b"].normalized_score == pytest.approx(20.0)


def test_raw_score_preserved():
    r = _make("svc", 7.5, fields=["env"])
    report = normalize_scores([r, _make("other", 0.0)])
    match = next(x for x in report.results if x.service == "svc")
    assert match.raw_score == pytest.approx(7.5)


def test_drifted_fields_carried_through():
    r = _make("svc", 3.0, fields=["replicas", "image"])
    report = normalize_scores([r, _make("clean", 0.0)])
    match = next(x for x in report.results if x.service == "svc")
    assert set(match.drifted_fields) == {"replicas", "image"}
    assert match.has_drift() is True


def test_clean_result_has_no_drift():
    r = _make("clean", 0.0)
    report = normalize_scores([r, _make("dirty", 5.0, fields=["x"])])
    match = next(x for x in report.results if x.service == "clean")
    assert match.has_drift() is False


def test_to_dict_contains_expected_keys():
    r = _make("svc", 4.0, fields=["port"])
    report = normalize_scores([r, _make("other", 0.0)])
    d = report.results[0].to_dict()
    assert set(d.keys()) == {"service", "raw_score", "normalized_score", "drifted_fields", "has_drift"}


def test_top_returns_highest_normalized():
    results = [_make(str(i), float(i)) for i in range(10)]
    report = normalize_scores(results)
    top = report.top(3)
    assert len(top) == 3
    assert top[0].normalized_score >= top[1].normalized_score >= top[2].normalized_score


def test_min_max_raw_recorded():
    results = [_make("a", 2.0), _make("b", 8.0), _make("c", 5.0)]
    report = normalize_scores(results)
    assert report.min_raw == pytest.approx(2.0)
    assert report.max_raw == pytest.approx(8.0)
