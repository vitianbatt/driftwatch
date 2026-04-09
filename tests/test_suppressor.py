"""Tests for driftwatch.suppressor."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.suppressor import (
    SuppressionError,
    SuppressionRule,
    apply_suppressions,
    load_rules_from_dicts,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


# ---------------------------------------------------------------------------
# SuppressionRule
# ---------------------------------------------------------------------------

class TestSuppressionRule:
    def test_empty_service_raises(self):
        with pytest.raises(SuppressionError):
            SuppressionRule(service="")

    def test_whitespace_service_raises(self):
        with pytest.raises(SuppressionError):
            SuppressionRule(service="   ")

    def test_exact_service_match_no_fields(self):
        rule = SuppressionRule(service="auth-service")
        assert rule.matches("auth-service", "replicas") is True

    def test_glob_service_match(self):
        rule = SuppressionRule(service="auth-*")
        assert rule.matches("auth-service", "replicas") is True
        assert rule.matches("payment-service", "replicas") is False

    def test_field_glob_match(self):
        rule = SuppressionRule(service="*", fields=["env.*"])
        assert rule.matches("any", "env.LOG_LEVEL") is True
        assert rule.matches("any", "replicas") is False

    def test_multiple_field_patterns(self):
        rule = SuppressionRule(service="*", fields=["replicas", "image"])
        assert rule.matches("svc", "replicas") is True
        assert rule.matches("svc", "image") is True
        assert rule.matches("svc", "memory") is False


# ---------------------------------------------------------------------------
# apply_suppressions
# ---------------------------------------------------------------------------

class TestApplySuppressions:
    def test_no_rules_returns_unchanged(self):
        results = [_make("svc", ["replicas"])]
        out = apply_suppressions(results, [])
        assert out[0].drifted_fields == ["replicas"]

    def test_suppresses_matching_field(self):
        rule = SuppressionRule(service="auth-service", fields=["replicas"])
        results = [_make("auth-service", ["replicas", "image"])]
        out = apply_suppressions(results, [rule])
        assert out[0].drifted_fields == ["image"]

    def test_fully_suppressed_result_has_empty_fields(self):
        rule = SuppressionRule(service="auth-service")
        results = [_make("auth-service", ["replicas", "image"])]
        out = apply_suppressions(results, [rule])
        assert out[0].drifted_fields == []

    def test_clean_result_passes_through(self):
        rule = SuppressionRule(service="*")
        results = [_make("svc", [])]
        out = apply_suppressions(results, [rule])
        assert out[0].drifted_fields == []

    def test_non_matching_service_untouched(self):
        rule = SuppressionRule(service="auth-*", fields=["replicas"])
        results = [_make("payment-service", ["replicas"])]
        out = apply_suppressions(results, [rule])
        assert out[0].drifted_fields == ["replicas"]


# ---------------------------------------------------------------------------
# load_rules_from_dicts
# ---------------------------------------------------------------------------

class TestLoadRulesFromDicts:
    def test_minimal_rule(self):
        rules = load_rules_from_dicts([{"service": "svc"}])
        assert len(rules) == 1
        assert rules[0].service == "svc"
        assert rules[0].fields == []
        assert rules[0].reason is None

    def test_full_rule(self):
        raw = [{"service": "auth-*", "fields": ["replicas"], "reason": "known"}]
        rules = load_rules_from_dicts(raw)
        assert rules[0].reason == "known"
        assert rules[0].fields == ["replicas"]

    def test_missing_service_raises(self):
        with pytest.raises(SuppressionError, match="index 0"):
            load_rules_from_dicts([{"fields": ["replicas"]}])
