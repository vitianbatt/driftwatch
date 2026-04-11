"""Tests for driftwatch.profiler."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.profiler import FieldProfile, ProfileReport, ProfilerError, build_profile


def _diff(fname: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=fname, kind=kind, expected="v", actual=None)


def _make(service: str, fields: list[str]) -> DriftResult:
    diffs = [_diff(f) for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# FieldProfile
# ---------------------------------------------------------------------------

class TestFieldProfile:
    def test_to_dict_keys(self):
        p = FieldProfile(field_name="timeout", drift_count=2, seen_in_services=["svc-a"])
        d = p.to_dict()
        assert set(d.keys()) == {"field_name", "drift_count", "seen_in_services"}

    def test_to_dict_values(self):
        p = FieldProfile(field_name="replicas", drift_count=3, seen_in_services=["b", "a"])
        d = p.to_dict()
        assert d["drift_count"] == 3
        assert d["seen_in_services"] == ["a", "b"]  # sorted


# ---------------------------------------------------------------------------
# build_profile
# ---------------------------------------------------------------------------

class TestBuildProfile:
    def test_none_raises(self):
        with pytest.raises(ProfilerError):
            build_profile(None)

    def test_empty_list_returns_empty_report(self):
        report = build_profile([])
        assert report.total_fields_tracked == 0

    def test_wrong_type_raises(self):
        with pytest.raises(ProfilerError):
            build_profile(["not-a-result"])

    def test_single_result_single_field(self):
        report = build_profile([_make("svc-a", ["timeout"])])
        assert "timeout" in report.profiles
        assert report.profiles["timeout"].drift_count == 1

    def test_same_field_across_services_accumulates(self):
        results = [
            _make("svc-a", ["timeout"]),
            _make("svc-b", ["timeout"]),
        ]
        report = build_profile(results)
        assert report.profiles["timeout"].drift_count == 2
        assert sorted(report.profiles["timeout"].seen_in_services) == ["svc-a", "svc-b"]

    def test_same_service_counted_once_per_field(self):
        # same service appearing twice should not duplicate seen_in_services
        results = [_make("svc-a", ["timeout"]), _make("svc-a", ["timeout"])]
        report = build_profile(results)
        assert report.profiles["timeout"].seen_in_services == ["svc-a"]

    def test_multiple_fields_tracked_independently(self):
        report = build_profile([_make("svc-a", ["timeout", "replicas"])])
        assert report.total_fields_tracked == 2

    def test_clean_result_contributes_nothing(self):
        report = build_profile([_make("svc-clean", [])])
        assert report.total_fields_tracked == 0


# ---------------------------------------------------------------------------
# ProfileReport helpers
# ---------------------------------------------------------------------------

class TestProfileReport:
    def test_top_negative_raises(self):
        report = ProfileReport()
        with pytest.raises(ProfilerError):
            report.top(-1)

    def test_top_zero_returns_empty(self):
        results = [_make("svc-a", ["timeout"])]
        report = build_profile(results)
        assert report.top(0) == []

    def test_top_returns_sorted_by_drift_count(self):
        results = [
            _make("svc-a", ["replicas"]),
            _make("svc-a", ["timeout"]),
            _make("svc-b", ["timeout"]),
        ]
        report = build_profile(results)
        top = report.top(2)
        assert top[0].field_name == "timeout"
        assert top[0].drift_count == 2

    def test_summary_empty(self):
        report = ProfileReport()
        assert report.summary() == "No field drift recorded."

    def test_summary_non_empty_contains_field(self):
        report = build_profile([_make("svc-a", ["timeout"])])
        s = report.summary()
        assert "timeout" in s
        assert "1 drift" in s
