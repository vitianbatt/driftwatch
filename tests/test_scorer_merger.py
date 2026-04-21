"""Tests for driftwatch.scorer_merger."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_merger import (
    ScorerMergerError,
    MergedScoredResult,
    MergedScoredReport,
    merge_scored_reports,
)


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="x", actual=None)


def _make(service: str, score: float, fields=None) -> ScoredResult:
    diffs = [_diff(f) for f in (fields or [])]
    result = DriftResult(service=service, diffs=diffs)
    return ScoredResult(result=result, score=score, drifted_fields=list(fields or []))


def _report(*results: ScoredResult) -> ScoredReport:
    return ScoredReport(results=list(results))


# ---------------------------------------------------------------------------
# MergedScoredResult
# ---------------------------------------------------------------------------
class TestMergedScoredResult:
    def test_has_drift_false_when_empty(self):
        r = MergedScoredResult(service="svc", score=0.0)
        assert r.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        r = MergedScoredResult(service="svc", score=3.0, drifted_fields=["env"])
        assert r.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        r = MergedScoredResult(service="svc", score=1.5, drifted_fields=["port"], source="primary", conflict=True)
        d = r.to_dict()
        assert set(d.keys()) == {"service", "score", "drifted_fields", "source", "conflict"}

    def test_to_dict_values(self):
        r = MergedScoredResult(service="auth", score=2.0, drifted_fields=["timeout"], source="secondary", conflict=False)
        d = r.to_dict()
        assert d["service"] == "auth"
        assert d["score"] == 2.0
        assert d["drifted_fields"] == ["timeout"]
        assert d["source"] == "secondary"
        assert d["conflict"] is False


# ---------------------------------------------------------------------------
# MergedScoredReport
# ---------------------------------------------------------------------------
class TestMergedScoredReport:
    def test_total_reflects_results(self):
        r = MergedScoredReport(results=[MergedScoredResult("a", 1.0), MergedScoredResult("b", 2.0)])
        assert r.total() == 2

    def test_average_score_empty(self):
        r = MergedScoredReport()
        assert r.average_score() == 0.0

    def test_average_score_calculated(self):
        r = MergedScoredReport(results=[MergedScoredResult("a", 2.0), MergedScoredResult("b", 4.0)])
        assert r.average_score() == 3.0

    def test_summary_string(self):
        r = MergedScoredReport(results=[MergedScoredResult("a", 1.0)], conflict_count=1)
        s = r.summary()
        assert "1 service" in s
        assert "1 conflict" in s


# ---------------------------------------------------------------------------
# merge_scored_reports
# ---------------------------------------------------------------------------
class TestMergeScoredReports:
    def test_none_primary_raises(self):
        with pytest.raises(ScorerMergerError):
            merge_scored_reports(None, _report())

    def test_none_secondary_raises(self):
        with pytest.raises(ScorerMergerError):
            merge_scored_reports(_report(), None)

    def test_empty_both_returns_empty(self):
        report = merge_scored_reports(_report(), _report())
        assert report.total() == 0
        assert report.conflict_count == 0

    def test_primary_only_service_kept(self):
        report = merge_scored_reports(_report(_make("auth", 2.0, ["env"])), _report())
        assert report.total() == 1
        assert report.results[0].service == "auth"
        assert report.results[0].source == "primary"

    def test_secondary_only_service_kept(self):
        report = merge_scored_reports(_report(), _report(_make("billing", 1.0, ["port"])))
        assert report.total() == 1
        assert report.results[0].service == "billing"
        assert report.results[0].source == "secondary"

    def test_no_conflict_when_scores_match(self):
        r = _make("svc", 3.0, ["env"])
        report = merge_scored_reports(_report(r), _report(r))
        assert report.conflict_count == 0
        assert report.results[0].conflict is False

    def test_conflict_detected_on_score_difference(self):
        p = _make("svc", 5.0, ["env"])
        s = _make("svc", 2.0, ["env"])
        report = merge_scored_reports(_report(p), _report(s))
        assert report.conflict_count == 1
        assert report.results[0].conflict is True

    def test_higher_score_wins_on_conflict(self):
        p = _make("svc", 5.0, ["env"])
        s = _make("svc", 2.0, ["port"])
        report = merge_scored_reports(_report(p), _report(s))
        assert report.results[0].score == 5.0
        assert report.results[0].source == "primary"

    def test_secondary_wins_when_higher(self):
        p = _make("svc", 1.0, ["env"])
        s = _make("svc", 7.0, ["port"])
        report = merge_scored_reports(_report(p), _report(s))
        assert report.results[0].score == 7.0
        assert report.results[0].source == "secondary"

    def test_services_sorted_alphabetically(self):
        p = _report(_make("zebra", 1.0), _make("alpha", 2.0))
        s = _report()
        report = merge_scored_reports(p, s)
        names = [r.service for r in report.results]
        assert names == sorted(names)

    def test_multiple_conflicts_counted(self):
        p = _report(_make("a", 1.0, ["x"]), _make("b", 3.0, ["y"]))
        s = _report(_make("a", 2.0, ["x"]), _make("b", 1.0, ["y"]))
        report = merge_scored_reports(p, s)
        assert report.conflict_count == 2
