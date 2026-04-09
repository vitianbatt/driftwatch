"""Tests for driftwatch.rollup."""
import pytest
from driftwatch.comparator import DriftResult
from driftwatch.rollup import RollupError, RollupReport, build_rollup
from driftwatch.filter import Severity


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or {})


# ---------------------------------------------------------------------------
# build_rollup
# ---------------------------------------------------------------------------

class TestBuildRollup:
    def test_empty_list_returns_zero_counts(self):
        report = build_rollup([])
        assert report.total == 0
        assert report.clean == 0
        assert report.drifted == 0

    def test_none_raises(self):
        with pytest.raises(RollupError):
            build_rollup(None)

    def test_all_clean(self):
        results = [_make("svc-a"), _make("svc-b")]
        report = build_rollup(results)
        assert report.total == 2
        assert report.clean == 2
        assert report.drifted == 0
        assert report.drifted_services == []

    def test_all_drifted(self):
        results = [
            _make("svc-a", {"env": ("prod", "staging")}),
            _make("svc-b", {"replicas": (3, 1)}),
        ]
        report = build_rollup(results)
        assert report.drifted == 2
        assert set(report.drifted_services) == {"svc-a", "svc-b"}

    def test_mixed_results(self):
        results = [
            _make("svc-a"),
            _make("svc-b", {"timeout": (30, 60)}),
        ]
        report = build_rollup(results)
        assert report.clean == 1
        assert report.drifted == 1

    def test_services_list_populated(self):
        results = [_make("alpha"), _make("beta"), _make("gamma")]
        report = build_rollup(results)
        assert report.services == ["alpha", "beta", "gamma"]

    def test_by_severity_keys_present(self):
        report = build_rollup([_make("svc")])
        for sev in Severity:
            assert sev.value in report.by_severity


# ---------------------------------------------------------------------------
# RollupReport helpers
# ---------------------------------------------------------------------------

class TestRollupReport:
    def _report(self, drifted=0):
        return RollupReport(
            total=3,
            clean=3 - drifted,
            drifted=drifted,
            by_severity={s.value: 0 for s in Severity},
            services=["a", "b", "c"],
            drifted_services=["a"] * drifted,
        )

    def test_has_any_drift_false_when_clean(self):
        assert self._report(drifted=0).has_any_drift() is False

    def test_has_any_drift_true_when_drifted(self):
        assert self._report(drifted=1).has_any_drift() is True

    def test_summary_contains_totals(self):
        text = self._report(drifted=1).summary()
        assert "Total services checked" in text
        assert "Drifted" in text

    def test_summary_lists_drifted_services(self):
        report = self._report(drifted=1)
        assert "a" in report.summary()
