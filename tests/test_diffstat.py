"""Tests for driftwatch.diffstat."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.diffstat import (
    DiffStatError,
    DiffStatReport,
    FieldStat,
    build_diffstat,
)


def _make(service: str, fields=None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


class TestFieldStat:
    def test_to_dict_keys(self):
        stat = FieldStat(field_name="timeout", occurrences=2, services=["svc-b", "svc-a"])
        d = stat.to_dict()
        assert set(d.keys()) == {"field_name", "occurrences", "services"}

    def test_to_dict_services_sorted(self):
        stat = FieldStat(field_name="port", occurrences=2, services=["svc-b", "svc-a"])
        assert stat.to_dict()["services"] == ["svc-a", "svc-b"]


class TestBuildDiffstat:
    def test_none_raises(self):
        with pytest.raises(DiffStatError):
            build_diffstat(None)

    def test_empty_list_returns_zero_counts(self):
        report = build_diffstat([])
        assert report.total_results == 0
        assert report.total_drifted == 0
        assert report.stats == {}

    def test_clean_result_not_counted(self):
        report = build_diffstat([_make("svc-a")])
        assert report.total_drifted == 0
        assert report.stats == {}

    def test_single_drift_field_recorded(self):
        report = build_diffstat([_make("svc-a", ["timeout"])])
        assert "timeout" in report.stats
        assert report.stats["timeout"].occurrences == 1
        assert "svc-a" in report.stats["timeout"].services

    def test_same_field_two_services(self):
        results = [_make("svc-a", ["port"]), _make("svc-b", ["port"])]
        report = build_diffstat(results)
        assert report.stats["port"].occurrences == 2
        assert sorted(report.stats["port"].services) == ["svc-a", "svc-b"]

    def test_service_not_duplicated_for_multiple_fields(self):
        results = [_make("svc-a", ["port", "timeout"]), _make("svc-a", ["port"])]
        report = build_diffstat(results)
        assert report.stats["port"].services.count("svc-a") == 1

    def test_total_drifted_counts_correctly(self):
        results = [_make("svc-a", ["x"]), _make("svc-b"), _make("svc-c", ["y"])]
        report = build_diffstat(results)
        assert report.total_drifted == 2

    def test_most_common_returns_sorted_desc(self):
        results = [
            _make("s1", ["a", "b"]),
            _make("s2", ["a"]),
            _make("s3", ["b", "c"]),
        ]
        report = build_diffstat(results)
        top = report.most_common(2)
        assert top[0].occurrences >= top[1].occurrences

    def test_most_common_respects_n(self):
        results = [_make(f"s{i}", [f"f{i}", "common"]) for i in range(6)]
        report = build_diffstat(results)
        assert len(report.most_common(3)) == 3

    def test_summary_no_drift(self):
        report = build_diffstat([])
        assert "No drift" in report.summary()

    def test_summary_with_drift(self):
        report = build_diffstat([_make("svc-a", ["timeout"])])
        text = report.summary()
        assert "timeout" in text
        assert "drifted" in text

    def test_invalid_item_raises(self):
        with pytest.raises(DiffStatError):
            build_diffstat([{"service": "bad"}])
