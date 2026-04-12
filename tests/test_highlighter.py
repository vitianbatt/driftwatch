"""Tests for driftwatch.highlighter."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.highlighter import (
    HighlightedResult,
    HighlighterError,
    HighlightRule,
    highlight_results,
)


def _diff(field_name: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field_name, kind=kind, expected="x", actual=None)


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [_diff(f) for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# TestHighlightRule
# ---------------------------------------------------------------------------

class TestHighlightRule:
    def test_valid_rule_created(self):
        rule = HighlightRule(pattern="timeout*", label="critical")
        assert rule.pattern == "timeout*"
        assert rule.label == "critical"

    def test_default_label_is_highlighted(self):
        rule = HighlightRule(pattern="*")
        assert rule.label == "highlighted"

    def test_empty_pattern_raises(self):
        with pytest.raises(HighlighterError):
            HighlightRule(pattern="")

    def test_whitespace_pattern_raises(self):
        with pytest.raises(HighlighterError):
            HighlightRule(pattern="   ")

    def test_empty_label_raises(self):
        with pytest.raises(HighlighterError):
            HighlightRule(pattern="*", label="")

    def test_matches_field_exact(self):
        rule = HighlightRule(pattern="replicas")
        assert rule.matches_field("replicas") is True
        assert rule.matches_field("timeout") is False

    def test_matches_field_glob(self):
        rule = HighlightRule(pattern="timeout*")
        assert rule.matches_field("timeout_ms") is True
        assert rule.matches_field("replicas") is False


# ---------------------------------------------------------------------------
# TestHighlightedResult
# ---------------------------------------------------------------------------

class TestHighlightedResult:
    def test_has_drift_false_when_no_diffs(self):
        r = HighlightedResult(service="svc", diffs=[], highlights={})
        assert r.has_drift() is False

    def test_has_drift_true_when_diffs_present(self):
        r = HighlightedResult(service="svc", diffs=[_diff("replicas")], highlights={})
        assert r.has_drift() is True

    def test_is_highlighted_true(self):
        r = HighlightedResult(service="svc", diffs=[], highlights={"replicas": "critical"})
        assert r.is_highlighted("replicas") is True

    def test_is_highlighted_false(self):
        r = HighlightedResult(service="svc", diffs=[], highlights={})
        assert r.is_highlighted("timeout") is False

    def test_to_dict_keys(self):
        r = HighlightedResult(service="svc", diffs=[_diff("replicas")], highlights={"replicas": "warn"})
        d = r.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "highlights", "drift_fields"}

    def test_to_dict_values(self):
        r = HighlightedResult(service="auth", diffs=[_diff("replicas")], highlights={"replicas": "critical"})
        d = r.to_dict()
        assert d["service"] == "auth"
        assert d["has_drift"] is True
        assert d["highlights"] == {"replicas": "critical"}
        assert d["drift_fields"] == ["replicas"]


# ---------------------------------------------------------------------------
# TestHighlightResults
# ---------------------------------------------------------------------------

class TestHighlightResults:
    def test_none_results_raises(self):
        with pytest.raises(HighlighterError):
            highlight_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(HighlighterError):
            highlight_results([], None)

    def test_empty_results_returns_empty(self):
        assert highlight_results([], []) == []

    def test_no_rules_no_highlights(self):
        results = [_make("svc", ["replicas"])]
        out = highlight_results(results, [])
        assert len(out) == 1
        assert out[0].highlights == {}

    def test_matching_rule_highlights_field(self):
        results = [_make("svc", ["replicas", "timeout_ms"])]
        rules = [HighlightRule(pattern="timeout*", label="warn")]
        out = highlight_results(results, rules)
        assert out[0].highlights == {"timeout_ms": "warn"}
        assert "replicas" not in out[0].highlights

    def test_multiple_rules_first_match_wins(self):
        results = [_make("svc", ["timeout_ms"])]
        rules = [
            HighlightRule(pattern="timeout*", label="first"),
            HighlightRule(pattern="*", label="second"),
        ]
        out = highlight_results(results, rules)
        assert out[0].highlights["timeout_ms"] == "first"

    def test_clean_service_has_no_highlights(self):
        results = [_make("clean-svc")]
        rules = [HighlightRule(pattern="*", label="warn")]
        out = highlight_results(results, rules)
        assert out[0].highlights == {}

    def test_multiple_services_processed(self):
        results = [_make("a", ["replicas"]), _make("b", ["timeout"])]
        rules = [HighlightRule(pattern="timeout", label="critical")]
        out = highlight_results(results, rules)
        assert out[0].highlights == {}
        assert out[1].highlights == {"timeout": "critical"}
