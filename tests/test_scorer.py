"""Tests for driftwatch/scorer.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from driftwatch.scorer import ScorerError, ScoredReport, score_results, _score_result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_diff(kind: str) -> MagicMock:
    d = MagicMock()
    d.kind = kind
    return d


def _make(service: str, *kinds: str):
    """Build a minimal DriftResult-like object."""
    r = MagicMock()
    r.service = service
    r.diffs = [_make_diff(k) for k in kinds] if kinds else []
    return r


# ---------------------------------------------------------------------------
# _score_result
# ---------------------------------------------------------------------------

class TestScoreResult:
    def test_no_diffs_returns_zero(self):
        r = _make("svc")
        assert _score_result(r) == 0

    def test_missing_weighted_three(self):
        r = _make("svc", "missing")
        assert _score_result(r) == 3

    def test_extra_weighted_one(self):
        r = _make("svc", "extra")
        assert _score_result(r) == 1

    def test_changed_weighted_two(self):
        r = _make("svc", "changed")
        assert _score_result(r) == 2

    def test_unknown_kind_weighted_one(self):
        r = _make("svc", "unknown")
        assert _score_result(r) == 1

    def test_mixed_diffs_summed(self):
        r = _make("svc", "missing", "changed", "extra")
        # 3 + 2 + 1 = 6
        assert _score_result(r) == 6


# ---------------------------------------------------------------------------
# score_results
# ---------------------------------------------------------------------------

class TestScoreResults:
    def test_none_raises(self):
        with pytest.raises(ScorerError):
            score_results(None)  # type: ignore[arg-type]

    def test_empty_list_returns_zero_counts(self):
        report = score_results([])
        assert report.total == 0
        assert report.service_count == 0
        assert report.scores == {}

    def test_single_clean_service(self):
        report = score_results([_make("auth")])
        assert report.scores["auth"] == 0
        assert report.total == 0
        assert report.service_count == 1

    def test_multiple_services_scored(self):
        results = [
            _make("auth", "missing"),      # 3
            _make("gateway", "changed"),   # 2
            _make("worker"),               # 0
        ]
        report = score_results(results)
        assert report.scores["auth"] == 3
        assert report.scores["gateway"] == 2
        assert report.scores["worker"] == 0
        assert report.total == 5
        assert report.service_count == 3

    def test_average_calculated_correctly(self):
        results = [_make("a", "missing"), _make("b", "extra")]
        report = score_results(results)  # 3 + 1 = 4 / 2 = 2.0
        assert report.average() == 2.0

    def test_highest_returns_worst_offender(self):
        results = [_make("low", "extra"), _make("high", "missing", "missing")]
        report = score_results(results)
        worst = report.highest()
        assert worst is not None
        assert worst[0] == "high"
        assert worst[1] == 6

    def test_highest_returns_none_when_empty(self):
        report = ScoredReport()
        assert report.highest() is None

    def test_summary_no_services(self):
        report = ScoredReport()
        assert report.summary() == "No services scored."

    def test_summary_with_services(self):
        results = [_make("svc", "changed")]
        report = score_results(results)
        s = report.summary()
        assert "svc" in s
        assert "total=2" in s
