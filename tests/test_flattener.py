"""Tests for driftwatch.flattener."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.flattener import (
    FlatRecord,
    FlatReport,
    FlattenerError,
    flatten_results,
)


def _diff(key: str, expected=None, actual=None, diff_type: str = "changed") -> FieldDiff:
    return FieldDiff(key=key, expected=expected, actual=actual, diff_type=diff_type)


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# FlatRecord
# ---------------------------------------------------------------------------

class TestFlatRecord:
    def test_to_dict_contains_all_keys(self):
        rec = FlatRecord(service="svc", key="port", expected=80, actual=8080, drift_type="changed")
        d = rec.to_dict()
        assert set(d.keys()) == {"service", "key", "expected", "actual", "drift_type"}

    def test_to_dict_values(self):
        rec = FlatRecord(service="svc", key="port", expected=80, actual=8080, drift_type="changed")
        d = rec.to_dict()
        assert d["service"] == "svc"
        assert d["key"] == "port"
        assert d["expected"] == 80
        assert d["actual"] == 8080
        assert d["drift_type"] == "changed"


# ---------------------------------------------------------------------------
# FlatReport
# ---------------------------------------------------------------------------

class TestFlatReport:
    def test_len_empty(self):
        assert len(FlatReport()) == 0

    def test_len_with_records(self):
        rec = FlatRecord(service="svc", key="k", expected=1, actual=2, drift_type="changed")
        assert len(FlatReport(records=[rec])) == 1

    def test_services_deduplicates(self):
        r1 = FlatRecord(service="a", key="k", expected=1, actual=2, drift_type="changed")
        r2 = FlatRecord(service="a", key="j", expected=3, actual=4, drift_type="changed")
        r3 = FlatRecord(service="b", key="k", expected=5, actual=6, drift_type="changed")
        report = FlatReport(records=[r1, r2, r3])
        assert report.services() == ["a", "b"]

    def test_for_service_filters(self):
        r1 = FlatRecord(service="a", key="k", expected=1, actual=2, drift_type="changed")
        r2 = FlatRecord(service="b", key="k", expected=3, actual=4, drift_type="changed")
        report = FlatReport(records=[r1, r2])
        assert report.for_service("a") == [r1]

    def test_summary_empty(self):
        assert FlatReport().summary() == "no drift records"

    def test_summary_with_records(self):
        r1 = FlatRecord(service="a", key="k", expected=1, actual=2, drift_type="changed")
        r2 = FlatRecord(service="b", key="k", expected=3, actual=4, drift_type="changed")
        report = FlatReport(records=[r1, r2])
        assert "2 flat record(s)" in report.summary()
        assert "2 service(s)" in report.summary()


# ---------------------------------------------------------------------------
# flatten_results
# ---------------------------------------------------------------------------

class TestFlattenResults:
    def test_none_raises(self):
        with pytest.raises(FlattenerError):
            flatten_results(None)  # type: ignore

    def test_empty_list_returns_empty_report(self):
        report = flatten_results([])
        assert len(report) == 0

    def test_clean_result_produces_no_records(self):
        report = flatten_results([_make("svc-a")])
        assert len(report) == 0

    def test_single_diff_produces_one_record(self):
        result = _make("svc-a", diffs=[_diff("port", expected=80, actual=8080)])
        report = flatten_results([result])
        assert len(report) == 1
        assert report.records[0].service == "svc-a"
        assert report.records[0].key == "port"

    def test_multiple_diffs_expand_correctly(self):
        diffs = [_diff("port"), _diff("timeout"), _diff("replicas")]
        report = flatten_results([_make("svc-a", diffs=diffs)])
        assert len(report) == 3

    def test_multiple_services_all_included(self):
        r1 = _make("svc-a", diffs=[_diff("port")])
        r2 = _make("svc-b", diffs=[_diff("image"), _diff("env")])
        report = flatten_results([r1, r2])
        assert len(report) == 3
        assert set(report.services()) == {"svc-a", "svc-b"}

    def test_diff_type_preserved(self):
        result = _make("svc-a", diffs=[_diff("port", diff_type="missing")])
        report = flatten_results([result])
        assert report.records[0].drift_type == "missing"
