"""Tests for driftwatch.ranker."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.filter import Severity
from driftwatch.ranker import RankedReport, RankerError, rank_results


def _diff(name: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=name, kind=kind, expected="x", actual="y")


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# rank_results – error cases
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(RankerError):
        rank_results(None)


def test_empty_list_returns_empty_report():
    report = rank_results([])
    assert isinstance(report, RankedReport)
    assert report.ranked == []


# ---------------------------------------------------------------------------
# Ranking order
# ---------------------------------------------------------------------------

def test_more_drifted_fields_ranks_higher():
    low = _make("svc-a", [_diff("f1")])
    high = _make("svc-b", [_diff("f1"), _diff("f2"), _diff("f3"), _diff("f4")])
    report = rank_results([low, high])
    assert report.ranked[0].result.service == "svc-b"
    assert report.ranked[1].result.service == "svc-a"


def test_clean_service_ranks_last():
    clean = _make("clean-svc")
    drifted = _make("drifted-svc", [_diff("env")])
    report = rank_results([clean, drifted])
    assert report.ranked[-1].result.service == "clean-svc"


def test_ranks_are_sequential():
    results = [_make(f"svc-{i}", [_diff("f")] * i) for i in range(5)]
    report = rank_results(results)
    for idx, rr in enumerate(report.ranked, start=1):
        assert rr.rank == idx


# ---------------------------------------------------------------------------
# Scores and severity
# ---------------------------------------------------------------------------

def test_clean_service_has_low_severity():
    report = rank_results([_make("svc")])
    assert report.ranked[0].severity == Severity.LOW


def test_high_drift_has_higher_score_than_low_drift():
    low = _make("svc-a", [_diff("f1")])
    high = _make("svc-b", [_diff("f1"), _diff("f2"), _diff("f3"), _diff("f4")])
    report = rank_results([low, high])
    assert report.ranked[0].score > report.ranked[1].score


def test_score_is_positive_for_drifted():
    result = _make("svc", [_diff("f1"), _diff("f2")])
    report = rank_results([result])
    assert report.ranked[0].score > 0


# ---------------------------------------------------------------------------
# top()
# ---------------------------------------------------------------------------

def test_top_returns_correct_count():
    results = [_make(f"svc-{i}", [_diff("f")] * i) for i in range(6)]
    report = rank_results(results)
    assert len(report.top(3)) == 3


def test_top_zero_returns_empty():
    results = [_make("svc", [_diff("f")])]
    report = rank_results(results)
    assert report.top(0) == []


def test_top_negative_raises():
    report = rank_results([_make("svc")])
    with pytest.raises(RankerError):
        report.top(-1)


def test_top_larger_than_list_returns_all():
    results = [_make("svc-a"), _make("svc-b")]
    report = rank_results(results)
    assert len(report.top(100)) == 2


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------

def test_to_dict_contains_expected_keys():
    result = _make("svc", [_diff("f1")])
    report = rank_results([result])
    d = report.ranked[0].to_dict()
    assert set(d.keys()) == {"service", "rank", "score", "severity", "drift_fields"}


def test_to_dict_drift_fields_count_matches():
    diffs = [_diff(f"f{i}") for i in range(4)]
    result = _make("svc", diffs)
    report = rank_results([result])
    assert report.ranked[0].to_dict()["drift_fields"] == 4


# ---------------------------------------------------------------------------
# summary()
# ---------------------------------------------------------------------------

def test_summary_empty_report():
    report = rank_results([])
    assert "No results" in report.summary()


def test_summary_contains_service_name():
    result = _make("my-service", [_diff("key")])
    report = rank_results([result])
    assert "my-service" in report.summary()
