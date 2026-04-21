"""Tests for driftwatch.scorer_ranker."""
import pytest

from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_ranker import (
    ScorerRankerError,
    RankedScoredResult,
    ScorerRankedReport,
    rank_scored_report,
)


def _diff(field: str = "replicas", kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="2", actual="3")


def _make(service: str, score: float, diffs=None) -> ScoredResult:
    return ScoredResult(
        service=service,
        score=score,
        has_drift=bool(diffs),
        diffs=diffs or [],
    )


def _report(*results: ScoredResult) -> ScoredReport:
    return ScoredReport(results=list(results))


class TestRankedScoredResult:
    def test_to_dict_contains_all_keys(self):
        r = RankedScoredResult(service="svc", score=4.5, rank=1, has_drift=True)
        d = r.to_dict()
        assert set(d.keys()) == {"service", "score", "rank", "has_drift"}

    def test_to_dict_values(self):
        r = RankedScoredResult(service="api", score=2.0, rank=3, has_drift=False)
        d = r.to_dict()
        assert d["service"] == "api"
        assert d["score"] == 2.0
        assert d["rank"] == 3
        assert d["has_drift"] is False


class TestScorerRankedReport:
    def test_len_reflects_results(self):
        rpt = ScorerRankedReport(results=[
            RankedScoredResult("a", 1.0, 1, False),
            RankedScoredResult("b", 0.5, 2, False),
        ])
        assert len(rpt) == 2

    def test_service_names_in_rank_order(self):
        rpt = ScorerRankedReport(results=[
            RankedScoredResult("high", 5.0, 1, True),
            RankedScoredResult("low", 1.0, 2, False),
        ])
        assert rpt.service_names() == ["high", "low"]

    def test_top_returns_correct_slice(self):
        rpt = ScorerRankedReport(results=[
            RankedScoredResult("a", 5.0, 1, True),
            RankedScoredResult("b", 3.0, 2, True),
            RankedScoredResult("c", 1.0, 3, False),
        ])
        assert len(rpt.top(2)) == 2
        assert rpt.top(2)[0].service == "a"

    def test_top_negative_raises(self):
        rpt = ScorerRankedReport()
        with pytest.raises(ScorerRankerError):
            rpt.top(-1)


class TestRankScoredReport:
    def test_none_raises(self):
        with pytest.raises(ScorerRankerError):
            rank_scored_report(None)

    def test_empty_report_returns_empty(self):
        rpt = rank_scored_report(_report())
        assert len(rpt) == 0

    def test_ranks_descending_by_score(self):
        rpt = rank_scored_report(_report(
            _make("low", 1.0),
            _make("high", 9.0, [_diff()]),
            _make("mid", 4.5, [_diff()]),
        ))
        names = rpt.service_names()
        assert names == ["high", "mid", "low"]

    def test_ranks_are_one_based(self):
        rpt = rank_scored_report(_report(
            _make("a", 3.0),
            _make("b", 1.0),
        ))
        assert rpt.results[0].rank == 1
        assert rpt.results[1].rank == 2

    def test_has_drift_preserved(self):
        rpt = rank_scored_report(_report(
            _make("drifted", 5.0, [_diff()]),
            _make("clean", 0.0),
        ))
        assert rpt.results[0].has_drift is True
        assert rpt.results[1].has_drift is False

    def test_single_result_gets_rank_one(self):
        rpt = rank_scored_report(_report(_make("only", 2.5)))
        assert rpt.results[0].rank == 1
