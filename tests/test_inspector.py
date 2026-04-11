"""Tests for driftwatch/inspector.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.inspector import (
    FieldOccurrence,
    InspectionReport,
    InspectorError,
    build_inspection,
)


def _diff(fname: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=fname, kind=kind, expected="x", actual=None)


def _make(service: str, *field_names: str) -> DriftResult:
    return DriftResult(service=service, diffs=[_diff(f) for f in field_names])


# ---------------------------------------------------------------------------
# FieldOccurrence
# ---------------------------------------------------------------------------

class TestFieldOccurrence:
    def test_to_dict_keys(self):
        occ = FieldOccurrence(field_name="timeout", count=2, services=["svc-a", "svc-b"])
        d = occ.to_dict()
        assert set(d.keys()) == {"field", "count", "services"}

    def test_to_dict_services_sorted(self):
        occ = FieldOccurrence(field_name="port", count=2, services=["z-svc", "a-svc"])
        assert occ.to_dict()["services"] == ["a-svc", "z-svc"]


# ---------------------------------------------------------------------------
# build_inspection
# ---------------------------------------------------------------------------

class TestBuildInspection:
    def test_none_raises(self):
        with pytest.raises(InspectorError):
            build_inspection(None)

    def test_empty_list_returns_empty_report(self):
        report = build_inspection([])
        assert report.total_fields_tracked() == 0
        assert report.occurrences == []

    def test_single_result_no_diffs(self):
        report = build_inspection([_make("svc-a")])
        assert report.total_fields_tracked() == 0

    def test_single_field_single_service(self):
        report = build_inspection([_make("svc-a", "timeout")])
        assert report.total_fields_tracked() == 1
        occ = report.lookup("timeout")
        assert occ is not None
        assert occ.count == 1
        assert occ.services == ["svc-a"]

    def test_same_field_multiple_services(self):
        results = [_make("svc-a", "timeout"), _make("svc-b", "timeout")]
        report = build_inspection(results)
        occ = report.lookup("timeout")
        assert occ.count == 2
        assert set(occ.services) == {"svc-a", "svc-b"}

    def test_multiple_fields(self):
        results = [_make("svc-a", "timeout", "port"), _make("svc-b", "port")]
        report = build_inspection(results)
        assert report.total_fields_tracked() == 2
        assert report.lookup("port").count == 2
        assert report.lookup("timeout").count == 1

    def test_duplicate_service_not_double_counted(self):
        results = [_make("svc-a", "timeout"), _make("svc-a", "timeout")]
        report = build_inspection(results)
        occ = report.lookup("timeout")
        assert occ.count == 1

    def test_most_common_returns_sorted_desc(self):
        results = [
            _make("s1", "alpha"),
            _make("s2", "alpha"),
            _make("s3", "alpha"),
            _make("s1", "beta"),
        ]
        report = build_inspection(results)
        top = report.most_common(2)
        assert top[0].field_name == "alpha"
        assert top[0].count == 3

    def test_summary_no_drift(self):
        report = build_inspection([])
        assert "No drift" in report.summary()

    def test_summary_with_drift(self):
        report = build_inspection([_make("svc-a", "timeout")])
        s = report.summary()
        assert "timeout" in s
        assert "svc-a" in s
