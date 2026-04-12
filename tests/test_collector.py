"""Tests for driftwatch.collector."""
import pytest

from driftwatch.collector import CollectedReport, CollectorError, collect
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [FieldDiff(field=f, kind="missing", expected="x", actual=None) for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# CollectedReport construction
# ---------------------------------------------------------------------------

class TestCollectedReport:
    def test_valid_report_created(self):
        r = CollectedReport(name="batch-1", results=[])
        assert r.name == "batch-1"
        assert r.results == []

    def test_empty_name_raises(self):
        with pytest.raises(CollectorError, match="name must not be empty"):
            CollectedReport(name="", results=[])

    def test_whitespace_name_raises(self):
        with pytest.raises(CollectorError, match="name must not be empty"):
            CollectedReport(name="   ", results=[])

    def test_none_results_raises(self):
        with pytest.raises(CollectorError, match="results must not be None"):
            CollectedReport(name="batch", results=None)  # type: ignore[arg-type]

    def test_len_reflects_results(self):
        report = CollectedReport(name="b", results=[_make("svc-a"), _make("svc-b")])
        assert len(report) == 2

    def test_service_names_order_preserved(self):
        report = CollectedReport(name="b", results=[_make("alpha"), _make("beta")])
        assert report.service_names() == ["alpha", "beta"]

    def test_drifted_filters_correctly(self):
        report = CollectedReport(
            name="b",
            results=[_make("clean"), _make("dirty", ["env"])],
        )
        assert [r.service for r in report.drifted()] == ["dirty"]

    def test_clean_filters_correctly(self):
        report = CollectedReport(
            name="b",
            results=[_make("clean"), _make("dirty", ["env"])],
        )
        assert [r.service for r in report.clean()] == ["clean"]

    def test_has_any_drift_false_when_all_clean(self):
        report = CollectedReport(name="b", results=[_make("svc")])
        assert report.has_any_drift() is False

    def test_has_any_drift_true_when_drift_present(self):
        report = CollectedReport(name="b", results=[_make("svc", ["key"])])
        assert report.has_any_drift() is True

    def test_summary_no_results(self):
        report = CollectedReport(name="run-1", results=[])
        assert "no results" in report.summary()

    def test_summary_all_clean(self):
        report = CollectedReport(name="run-1", results=[_make("a"), _make("b")])
        assert "all 2" in report.summary()
        assert "clean" in report.summary()

    def test_summary_with_drift(self):
        report = CollectedReport(
            name="run-1",
            results=[_make("a"), _make("b", ["x"])],
        )
        assert "1/2" in report.summary()


# ---------------------------------------------------------------------------
# collect() helper
# ---------------------------------------------------------------------------

def test_collect_wraps_results():
    results = [_make("svc-x")]
    report = collect("my-batch", results)
    assert isinstance(report, CollectedReport)
    assert len(report) == 1


def test_collect_none_raises():
    with pytest.raises(CollectorError, match="Cannot collect None"):
        collect("batch", None)  # type: ignore[arg-type]


def test_collect_returns_copy_of_list():
    original = [_make("svc")]
    report = collect("b", original)
    original.clear()
    assert len(report) == 1
