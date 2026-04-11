"""Tests for driftwatch.curator."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.curator import CuratedReport, CuratorError, curate


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [FieldDiff(field=f, expected="a", actual="b", diff_type="changed") for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# CuratedReport
# ---------------------------------------------------------------------------

class TestCuratedReport:
    def test_len_reflects_results(self):
        report = CuratedReport(results=[_make("svc-a"), _make("svc-b")], dropped=1)
        assert len(report) == 2

    def test_service_names_order_preserved(self):
        report = CuratedReport(results=[_make("alpha"), _make("beta")], dropped=0)
        assert report.service_names() == ["alpha", "beta"]

    def test_summary_no_results(self):
        report = CuratedReport(results=[], dropped=3)
        assert "No results retained" in report.summary()
        assert "3" in report.summary()

    def test_summary_with_drift(self):
        report = CuratedReport(
            results=[_make("svc-a", ["timeout"]), _make("svc-b")],
            dropped=0,
        )
        text = report.summary()
        assert "2 result" in text
        assert "1 service" in text

    def test_summary_no_drift(self):
        report = CuratedReport(results=[_make("svc-a"), _make("svc-b")], dropped=1)
        assert "0 service" in report.summary()
        assert "1 duplicate" in report.summary()


# ---------------------------------------------------------------------------
# curate()
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(CuratorError):
        curate(None)  # type: ignore[arg-type]


def test_empty_list_returns_empty_report():
    report = curate([])
    assert len(report) == 0
    assert report.dropped == 0


def test_no_duplicates_unchanged():
    results = [_make("svc-a"), _make("svc-b"), _make("svc-c")]
    report = curate(results)
    assert len(report) == 3
    assert report.dropped == 0


def test_duplicate_service_last_wins():
    r1 = _make("svc-a", ["timeout"])
    r2 = _make("svc-a")  # no drift – should win
    report = curate([r1, r2])
    assert len(report) == 1
    assert report.dropped == 1
    assert report.results[0].diffs == []


def test_multiple_duplicates_counted():
    results = [_make("svc-a"), _make("svc-a"), _make("svc-a"), _make("svc-b")]
    report = curate(results)
    assert len(report) == 2
    assert report.dropped == 2


def test_service_names_unique_after_curation():
    results = [_make("x"), _make("y"), _make("x")]
    report = curate(results)
    names = report.service_names()
    assert sorted(names) == ["x", "y"]
