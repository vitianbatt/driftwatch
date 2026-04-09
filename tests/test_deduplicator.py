"""Tests for driftwatch.deduplicator."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.deduplicator import (
    DeduplicatorError,
    DeduplicatedReport,
    deduplicate,
)


def _make(service: str, diffs: dict | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or {})


# ---------------------------------------------------------------------------
# TestDeduplicatedReport
# ---------------------------------------------------------------------------

class TestDeduplicatedReport:
    def test_total_seen_sums_unique_and_duplicates(self):
        report = DeduplicatedReport(unique=[_make("svc-a")], duplicate_count=3)
        assert report.total_seen() == 4

    def test_summary_empty_unique(self):
        report = DeduplicatedReport(unique=[], duplicate_count=2)
        assert "No results" in report.summary()

    def test_summary_lists_services(self):
        report = DeduplicatedReport(
            unique=[_make("auth"), _make("billing", {"timeout": "differs"})],
            duplicate_count=1,
        )
        text = report.summary()
        assert "auth" in text
        assert "billing" in text
        assert "DRIFT" in text
        assert "OK" in text

    def test_summary_shows_counts(self):
        report = DeduplicatedReport(unique=[_make("svc")], duplicate_count=5)
        assert "5" in report.summary()


# ---------------------------------------------------------------------------
# TestDeduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_empty_list_returns_empty_report(self):
        report = deduplicate([])
        assert report.unique == []
        assert report.duplicate_count == 0

    def test_none_raises(self):
        with pytest.raises(DeduplicatorError):
            deduplicate(None)  # type: ignore

    def test_invalid_item_raises(self):
        with pytest.raises(DeduplicatorError, match="DriftResult"):
            deduplicate(["not-a-result"])  # type: ignore

    def test_single_result_no_duplicates(self):
        results = [_make("auth")]
        report = deduplicate(results)
        assert len(report.unique) == 1
        assert report.duplicate_count == 0

    def test_identical_results_deduped(self):
        r = _make("auth")
        report = deduplicate([r, r])
        assert len(report.unique) == 1
        assert report.duplicate_count == 1

    def test_same_service_different_diffs_kept_separate(self):
        r1 = _make("auth", {"timeout": "differs"})
        r2 = _make("auth", {"retries": "differs"})
        report = deduplicate([r1, r2])
        assert len(report.unique) == 2
        assert report.duplicate_count == 0

    def test_different_services_all_kept(self):
        results = [_make("auth"), _make("billing"), _make("gateway")]
        report = deduplicate(results)
        assert len(report.unique) == 3
        assert report.duplicate_count == 0

    def test_multiple_duplicates_counted_correctly(self):
        r = _make("svc", {"key": "v"})
        report = deduplicate([r, r, r, r])
        assert len(report.unique) == 1
        assert report.duplicate_count == 3
        assert report.total_seen() == 4

    def test_preserves_first_occurrence_order(self):
        r1 = _make("alpha")
        r2 = _make("beta")
        r3 = _make("alpha")  # duplicate of r1
        report = deduplicate([r1, r2, r3])
        assert report.unique[0].service == "alpha"
        assert report.unique[1].service == "beta"
