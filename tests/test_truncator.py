"""Tests for driftwatch.truncator."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.truncator import (
    TruncateConfig,
    TruncatedResult,
    TruncateReport,
    TruncatorError,
    truncate_results,
)


def _diff(field: str, kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="a", actual="b")


def _make(service: str, fields: list[str]) -> DriftResult:
    diffs = [_diff(f) for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# TruncateConfig
# ---------------------------------------------------------------------------

class TestTruncateConfig:
    def test_default_max_diffs(self):
        cfg = TruncateConfig()
        assert cfg.max_diffs == 10

    def test_custom_max_diffs(self):
        cfg = TruncateConfig(max_diffs=3)
        assert cfg.max_diffs == 3

    def test_zero_max_diffs_raises(self):
        with pytest.raises(TruncatorError):
            TruncateConfig(max_diffs=0)

    def test_negative_max_diffs_raises(self):
        with pytest.raises(TruncatorError):
            TruncateConfig(max_diffs=-1)


# ---------------------------------------------------------------------------
# TruncatedResult
# ---------------------------------------------------------------------------

class TestTruncatedResult:
    def test_has_drift_false_when_empty(self):
        r = TruncatedResult(service="svc", diffs=[])
        assert not r.has_drift

    def test_has_drift_true_when_diffs_present(self):
        r = TruncatedResult(service="svc", diffs=[_diff("x")])
        assert r.has_drift

    def test_has_drift_true_when_only_truncated(self):
        r = TruncatedResult(service="svc", diffs=[], truncated_count=2)
        assert r.has_drift

    def test_was_truncated_false_by_default(self):
        r = TruncatedResult(service="svc", diffs=[])
        assert not r.was_truncated

    def test_was_truncated_true_when_count_positive(self):
        r = TruncatedResult(service="svc", diffs=[], truncated_count=1)
        assert r.was_truncated

    def test_summary_no_truncation(self):
        r = TruncatedResult(service="svc", diffs=[_diff("x")])
        assert "svc" in r.summary()
        assert "1 diff" in r.summary()
        assert "suppressed" not in r.summary()

    def test_summary_with_truncation(self):
        r = TruncatedResult(service="svc", diffs=[_diff("x")], truncated_count=3)
        assert "3 suppressed" in r.summary()

    def test_to_dict_keys(self):
        r = TruncatedResult(service="svc", diffs=[_diff("x")], truncated_count=1)
        d = r.to_dict()
        assert set(d.keys()) == {"service", "diffs", "truncated_count", "was_truncated"}


# ---------------------------------------------------------------------------
# truncate_results
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(TruncatorError):
        truncate_results(None)


def test_empty_list_returns_empty_report():
    report = truncate_results([])
    assert report.total == 0
    assert not report.any_truncated


def test_no_truncation_when_under_limit():
    results = [_make("svc", ["a", "b", "c"])]
    report = truncate_results(results, TruncateConfig(max_diffs=5))
    assert report.total == 1
    assert not report.results[0].was_truncated
    assert len(report.results[0].diffs) == 3


def test_truncation_applied_when_over_limit():
    results = [_make("svc", ["a", "b", "c", "d", "e"])]
    report = truncate_results(results, TruncateConfig(max_diffs=3))
    r = report.results[0]
    assert len(r.diffs) == 3
    assert r.truncated_count == 2
    assert r.was_truncated


def test_multiple_services_truncated_independently():
    results = [
        _make("alpha", ["x", "y", "z"]),
        _make("beta", ["p"]),
    ]
    report = truncate_results(results, TruncateConfig(max_diffs=2))
    alpha = next(r for r in report.results if r.service == "alpha")
    beta = next(r for r in report.results if r.service == "beta")
    assert alpha.truncated_count == 1
    assert not beta.was_truncated


def test_any_truncated_false_when_all_within_limit():
    results = [_make("svc", ["a"])]
    report = truncate_results(results, TruncateConfig(max_diffs=5))
    assert not report.any_truncated


def test_any_truncated_true_when_at_least_one_over_limit():
    results = [_make("svc", ["a", "b", "c"])]
    report = truncate_results(results, TruncateConfig(max_diffs=2))
    assert report.any_truncated


def test_default_config_used_when_none_provided():
    fields = [str(i) for i in range(15)]
    results = [_make("svc", fields)]
    report = truncate_results(results)
    r = report.results[0]
    assert len(r.diffs) == 10
    assert r.truncated_count == 5
