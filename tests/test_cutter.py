"""Tests for driftwatch.cutter."""
import pytest
from driftwatch.cutter import (
    CutConfig,
    CutReport,
    CutResult,
    CutterError,
    cut_results,
)
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _diff(field: str, expected="a", actual="b") -> FieldDiff:
    return FieldDiff(field=field, expected=expected, actual=actual)


def _make(service: str, diffs=None) -> DriftResult:
    r = DriftResult(service=service)
    r.diffs = diffs or []
    return r


# --- CutConfig ---

class TestCutConfig:
    def test_prefix_only_valid(self):
        cfg = CutConfig(prefix="env_")
        assert cfg.prefix == "env_"

    def test_suffix_only_valid(self):
        cfg = CutConfig(suffix="_timeout")
        assert cfg.suffix == "_timeout"

    def test_both_valid(self):
        cfg = CutConfig(prefix="env_", suffix="_timeout")
        assert cfg.prefix == "env_"
        assert cfg.suffix == "_timeout"

    def test_neither_raises(self):
        with pytest.raises(CutterError, match="at least one"):
            CutConfig()

    def test_blank_prefix_raises(self):
        with pytest.raises(CutterError, match="prefix"):
            CutConfig(prefix="   ")

    def test_blank_suffix_raises(self):
        with pytest.raises(CutterError, match="suffix"):
            CutConfig(suffix="")


# --- CutResult ---

class TestCutResult:
    def test_has_drift_false_when_empty(self):
        r = CutResult(service="svc")
        assert r.has_drift() is False

    def test_has_drift_true_when_diffs(self):
        r = CutResult(service="svc", diffs=[_diff("env_host")])
        assert r.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        r = CutResult(service="svc", diffs=[_diff("env_host")])
        d = r.to_dict()
        assert "service" in d
        assert "has_drift" in d
        assert "diffs" in d


# --- cut_results ---

def test_none_results_raises():
    with pytest.raises(CutterError):
        cut_results(None, CutConfig(prefix="x"))


def test_none_config_raises():
    with pytest.raises(CutterError):
        cut_results([], None)


def test_empty_list_returns_empty_report():
    report = cut_results([], CutConfig(prefix="env_"))
    assert len(report) == 0


def test_prefix_filters_correctly():
    results = [
        _make("svc", [_diff("env_host"), _diff("db_port"), _diff("env_timeout")])
    ]
    report = cut_results(results, CutConfig(prefix="env_"))
    assert len(report.results[0].diffs) == 2
    assert all(d.field.startswith("env_") for d in report.results[0].diffs)


def test_suffix_filters_correctly():
    results = [
        _make("svc", [_diff("host_timeout"), _diff("db_port"), _diff("conn_timeout")])
    ]
    report = cut_results(results, CutConfig(suffix="_timeout"))
    assert len(report.results[0].diffs) == 2


def test_prefix_and_suffix_combined():
    results = [
        _make("svc", [_diff("env_timeout"), _diff("env_host"), _diff("db_timeout")])
    ]
    report = cut_results(results, CutConfig(prefix="env_", suffix="_timeout"))
    assert len(report.results[0].diffs) == 1
    assert report.results[0].diffs[0].field == "env_timeout"


def test_no_matching_fields_gives_no_drift():
    results = [_make("svc", [_diff("db_host")])]
    report = cut_results(results, CutConfig(prefix="env_"))
    assert report.results[0].has_drift() is False


def test_clean_result_stays_clean():
    results = [_make("svc", [])]
    report = cut_results(results, CutConfig(prefix="env_"))
    assert report.results[0].has_drift() is False


def test_total_with_drift_counts_correctly():
    results = [
        _make("svc-a", [_diff("env_host")]),
        _make("svc-b", [_diff("db_port")]),
    ]
    report = cut_results(results, CutConfig(prefix="env_"))
    assert report.total_with_drift() == 1


def test_service_names_preserved():
    results = [_make("alpha"), _make("beta")]
    report = cut_results(results, CutConfig(suffix="_port"))
    assert report.service_names() == ["alpha", "beta"]
