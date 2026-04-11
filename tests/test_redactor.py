"""Tests for driftwatch.redactor."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.redactor import (
    RedactedResult,
    RedactRule,
    RedactorError,
    redact_results,
)


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field: str, kind: str = "changed", expected="old", actual="new") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=expected, actual=actual)


# ---------------------------------------------------------------------------
# TestRedactRule
# ---------------------------------------------------------------------------

class TestRedactRule:
    def test_valid_rule_created(self):
        rule = RedactRule(pattern="password")
        assert rule.pattern == "password"
        assert rule.mask == "***REDACTED***"

    def test_custom_mask(self):
        rule = RedactRule(pattern="secret", mask="[hidden]")
        assert rule.mask == "[hidden]"

    def test_empty_pattern_raises(self):
        with pytest.raises(RedactorError, match="non-empty"):
            RedactRule(pattern="")

    def test_whitespace_pattern_raises(self):
        with pytest.raises(RedactorError, match="non-empty"):
            RedactRule(pattern="   ")

    def test_invalid_regex_raises(self):
        with pytest.raises(RedactorError, match="invalid regex"):
            RedactRule(pattern="[unclosed")

    def test_matches_exact(self):
        rule = RedactRule(pattern="^password$")
        assert rule.matches("password")
        assert not rule.matches("password_hash")

    def test_matches_substring(self):
        rule = RedactRule(pattern="secret")
        assert rule.matches("api_secret_key")
        assert not rule.matches("public_key")


# ---------------------------------------------------------------------------
# TestRedactResults
# ---------------------------------------------------------------------------

class TestRedactResults:
    def test_empty_results_returns_empty(self):
        assert redact_results([], [RedactRule(pattern="pass")]) == []

    def test_none_results_raises(self):
        with pytest.raises(RedactorError):
            redact_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(RedactorError):
            redact_results([], None)

    def test_no_rules_leaves_diffs_unchanged(self):
        result = _make("svc", [_diff("replicas")])
        out = redact_results([result], [])
        assert len(out) == 1
        assert out[0].diffs[0].expected == "old"
        assert out[0].redacted_fields == []

    def test_matching_rule_masks_values(self):
        result = _make("svc", [_diff("db_password", expected="hunter2", actual="secret")])
        rules = [RedactRule(pattern="password")]
        out = redact_results([result], rules)
        d = out[0].diffs[0]
        assert d.expected == "***REDACTED***"
        assert d.actual == "***REDACTED***"

    def test_redacted_fields_recorded(self):
        result = _make("svc", [_diff("api_key"), _diff("replicas")])
        rules = [RedactRule(pattern="api_key")]
        out = redact_results([result], rules)
        assert "api_key" in out[0].redacted_fields
        assert "replicas" not in out[0].redacted_fields

    def test_non_matching_diff_preserved(self):
        result = _make("svc", [_diff("image", expected="v1", actual="v2")])
        rules = [RedactRule(pattern="password")]
        out = redact_results([result], rules)
        assert out[0].diffs[0].expected == "v1"

    def test_has_drift_false_when_no_diffs(self):
        out = redact_results([_make("svc")], [])
        assert not out[0].has_drift

    def test_has_drift_true_with_diffs(self):
        out = redact_results([_make("svc", [_diff("x")])], [])
        assert out[0].has_drift

    def test_to_dict_structure(self):
        result = _make("svc", [_diff("token")])
        rules = [RedactRule(pattern="token")]
        d = redact_results([result], rules)[0].to_dict()
        assert d["service"] == "svc"
        assert d["redacted_fields"] == ["token"]
        assert d["diffs"][0]["expected"] == "***REDACTED***"
