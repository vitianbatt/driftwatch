"""Tests for driftwatch.scoper."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scoper import (
    ScopeConfig,
    ScopedReport,
    ScoperError,
    apply_scope,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, drifted: bool = False) -> DriftResult:
    diffs = [FieldDiff(field="env", expected="prod", actual="dev")] if drifted else []
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# ScopeConfig validation
# ---------------------------------------------------------------------------

class TestScopeConfig:
    def test_empty_config_is_valid(self):
        cfg = ScopeConfig()
        assert cfg.include == []
        assert cfg.exclude == []

    def test_include_only_valid(self):
        cfg = ScopeConfig(include=["auth", "billing"])
        assert "auth" in cfg.include

    def test_exclude_only_valid(self):
        cfg = ScopeConfig(exclude=["legacy"])
        assert "legacy" in cfg.exclude

    def test_overlap_raises(self):
        with pytest.raises(ScoperError, match="both include and exclude"):
            ScopeConfig(include=["auth"], exclude=["auth"])

    def test_include_not_list_raises(self):
        with pytest.raises(ScoperError, match="include must be a list"):
            ScopeConfig(include="auth")  # type: ignore[arg-type]

    def test_exclude_not_list_raises(self):
        with pytest.raises(ScoperError, match="exclude must be a list"):
            ScopeConfig(exclude="legacy")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# apply_scope
# ---------------------------------------------------------------------------

class TestApplyScope:
    def test_none_results_raises(self):
        with pytest.raises(ScoperError):
            apply_scope(None, ScopeConfig())  # type: ignore[arg-type]

    def test_empty_config_puts_all_in_scope(self):
        results = [_make("auth"), _make("billing")]
        report = apply_scope(results, ScopeConfig())
        assert report.total_in_scope == 2
        assert report.total_out_of_scope == 0

    def test_include_filters_non_listed(self):
        results = [_make("auth"), _make("billing"), _make("legacy")]
        cfg = ScopeConfig(include=["auth", "billing"])
        report = apply_scope(results, cfg)
        in_names = [r.service for r in report.in_scope]
        assert "legacy" not in in_names
        assert report.total_in_scope == 2
        assert report.total_out_of_scope == 1

    def test_exclude_removes_listed_service(self):
        results = [_make("auth"), _make("legacy")]
        cfg = ScopeConfig(exclude=["legacy"])
        report = apply_scope(results, cfg)
        assert report.total_in_scope == 1
        assert report.in_scope[0].service == "auth"

    def test_empty_results_returns_empty_report(self):
        report = apply_scope([], ScopeConfig())
        assert report.total_in_scope == 0
        assert report.total_out_of_scope == 0


# ---------------------------------------------------------------------------
# ScopedReport
# ---------------------------------------------------------------------------

class TestScopedReport:
    def test_summary_all_clean(self):
        report = ScopedReport(in_scope=[_make("auth")], out_of_scope=[])
        assert "1 in scope" in report.summary()
        assert "0 drifted" in report.summary()

    def test_summary_with_drift(self):
        report = ScopedReport(
            in_scope=[_make("auth", drifted=True), _make("billing")],
            out_of_scope=[_make("legacy")],
        )
        s = report.summary()
        assert "2 in scope" in s
        assert "1 drifted" in s
        assert "1 out of scope" in s
