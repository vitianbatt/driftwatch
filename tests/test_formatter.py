"""Tests for driftwatch/formatter.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.formatter import (
    FormatterError,
    FormattedRecord,
    format_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _diff(field, expected="spec_val", actual="live_val"):
    return FieldDiff(field=field, expected=expected, actual=actual)


def _make(service, diffs=None):
    diffs = diffs or []
    return DriftResult(
        service=service,
        has_drift=bool(diffs),
        diffs=diffs,
    )


# ---------------------------------------------------------------------------
# FormattedRecord
# ---------------------------------------------------------------------------

class TestFormattedRecord:
    def test_to_dict_contains_all_keys(self):
        rec = FormattedRecord(service="svc", has_drift=False, drift_count=0)
        d = rec.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "drift_count", "field_summaries"}

    def test_one_line_no_drift(self):
        rec = FormattedRecord(service="auth", has_drift=False, drift_count=0)
        assert rec.one_line() == "auth: OK"

    def test_one_line_with_drift(self):
        rec = FormattedRecord(
            service="api",
            has_drift=True,
            drift_count=2,
            field_summaries=["~replicas", "-timeout"],
        )
        line = rec.one_line()
        assert "api: DRIFT" in line
        assert "~replicas" in line
        assert "-timeout" in line

    def test_one_line_truncates_beyond_three(self):
        rec = FormattedRecord(
            service="svc",
            has_drift=True,
            drift_count=5,
            field_summaries=["~a", "~b", "~c", "~d", "~e"],
        )
        line = rec.one_line()
        assert "+2 more" in line


# ---------------------------------------------------------------------------
# format_results
# ---------------------------------------------------------------------------

class TestFormatResults:
    def test_none_raises(self):
        with pytest.raises(FormatterError):
            format_results(None)

    def test_empty_list_returns_empty(self):
        assert format_results([]) == []

    def test_clean_result_has_no_drift(self):
        records = format_results([_make("auth")])
        assert len(records) == 1
        assert records[0].has_drift is False
        assert records[0].drift_count == 0

    def test_drift_result_counted(self):
        records = format_results([_make("api", diffs=[_diff("replicas"), _diff("image")])])
        assert records[0].drift_count == 2

    def test_missing_field_summary_prefix(self):
        d = FieldDiff(field="timeout", expected=None, actual="30s")
        records = format_results([_make("svc", diffs=[d])])
        assert "+timeout" in records[0].field_summaries

    def test_extra_field_summary_prefix(self):
        d = FieldDiff(field="debug", expected="true", actual=None)
        records = format_results([_make("svc", diffs=[d])])
        assert "-debug" in records[0].field_summaries

    def test_changed_field_summary_prefix(self):
        d = FieldDiff(field="replicas", expected="3", actual="1")
        records = format_results([_make("svc", diffs=[d])])
        assert "~replicas" in records[0].field_summaries

    def test_multiple_results_preserved_order(self):
        results = [_make("a"), _make("b"), _make("c")]
        records = format_results(results)
        assert [r.service for r in records] == ["a", "b", "c"]

    def test_raw_result_attached(self):
        r = _make("auth")
        records = format_results([r])
        assert records[0].raw_result is r
