"""Tests for driftwatch.scorer_comparator."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredReport, ScoredResult
from driftwatch.scorer_comparator import (
    ScorerComparatorError,
    ScoreDelta,
    ScorerComparisonReport,
    compare_scored_reports,
)


def _diff(field: str = "timeout") -> FieldDiff:
    return FieldDiff(field=field, expected="10", actual="30", kind="changed")


def _make(service: str, score: float, diffs=None) -> ScoredResult:
    result = DriftResult(service=service, diffs=diffs or [])
    return ScoredResult(result=result, score=score)


def _report(*results: ScoredResult) -> ScoredReport:
    return ScoredReport(results=list(results))


class TestScoreDelta:
    def test_delta_positive_when_regressed(self):
        d = ScoreDelta(service="svc", previous_score=2.0, current_score=5.0)
        assert d.delta == pytest.approx(3.0)

    def test_delta_negative_when_improved(self):
        d = ScoreDelta(service="svc", previous_score=5.0, current_score=2.0)
        assert d.delta == pytest.approx(-3.0)

    def test_regressed_true_when_score_increased(self):
        d = ScoreDelta(service="svc", previous_score=1.0, current_score=4.0)
        assert d.regressed is True
        assert d.improved is False

    def test_improved_true_when_score_decreased(self):
        d = ScoreDelta(service="svc", previous_score=4.0, current_score=1.0)
        assert d.improved is True
        assert d.regressed is False

    def test_to_dict_contains_all_keys(self):
        d = ScoreDelta(service="auth", previous_score=3.0, current_score=6.0)
        result = d.to_dict()
        assert set(result.keys()) == {
            "service", "previous_score", "current_score", "delta", "improved", "regressed"
        }

    def test_to_dict_delta_rounded(self):
        d = ScoreDelta(service="svc", previous_score=1.0, current_score=1.333333333)
        assert d.to_dict()["delta"] == pytest.approx(0.3333, abs=1e-3)


class TestCompareScoredReports:
    def test_none_previous_raises(self):
        curr = _report(_make("svc", 1.0))
        with pytest.raises(ScorerComparatorError):
            compare_scored_reports(None, curr)

    def test_none_current_raises(self):
        prev = _report(_make("svc", 1.0))
        with pytest.raises(ScorerComparatorError):
            compare_scored_reports(prev, None)

    def test_identical_reports_produce_no_deltas(self):
        r = _make("svc", 2.0)
        report = compare_scored_reports(_report(r), _report(r))
        assert report.deltas == []
        assert report.new_services == []
        assert report.dropped_services == []

    def test_score_change_produces_delta(self):
        prev = _report(_make("auth", 1.0))
        curr = _report(_make("auth", 4.0))
        report = compare_scored_reports(prev, curr)
        assert len(report.deltas) == 1
        assert report.deltas[0].service == "auth"
        assert report.deltas[0].previous_score == pytest.approx(1.0)
        assert report.deltas[0].current_score == pytest.approx(4.0)

    def test_new_service_detected(self):
        prev = _report(_make("auth", 1.0))
        curr = _report(_make("auth", 1.0), _make("payments", 2.0))
        report = compare_scored_reports(prev, curr)
        assert "payments" in report.new_services

    def test_dropped_service_detected(self):
        prev = _report(_make("auth", 1.0), _make("legacy", 3.0))
        curr = _report(_make("auth", 1.0))
        report = compare_scored_reports(prev, curr)
        assert "legacy" in report.dropped_services

    def test_has_regressions_false_when_no_deltas(self):
        prev = _report(_make("svc", 1.0))
        curr = _report(_make("svc", 1.0))
        report = compare_scored_reports(prev, curr)
        assert report.has_regressions() is False

    def test_has_regressions_true_when_score_increased(self):
        prev = _report(_make("svc", 1.0))
        curr = _report(_make("svc", 5.0))
        report = compare_scored_reports(prev, curr)
        assert report.has_regressions() is True


class TestScorerComparisonReportSummary:
    def test_summary_no_changes(self):
        report = ScorerComparisonReport()
        assert report.summary() == "No changes between reports."

    def test_summary_lists_regressions(self):
        delta = ScoreDelta(service="svc", previous_score=1.0, current_score=4.0)
        report = ScorerComparisonReport(deltas=[delta])
        assert "regression" in report.summary()

    def test_summary_lists_improvements(self):
        delta = ScoreDelta(service="svc", previous_score=4.0, current_score=1.0)
        report = ScorerComparisonReport(deltas=[delta])
        assert "improvement" in report.summary()

    def test_summary_includes_new_and_dropped(self):
        report = ScorerComparisonReport(
            new_services=["svc-a"],
            dropped_services=["svc-b"],
        )
        summary = report.summary()
        assert "new" in summary
        assert "dropped" in summary
