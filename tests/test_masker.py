"""Tests for driftwatch.masker."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.masker import (
    MaskerError,
    MaskRule,
    MaskedResult,
    mask_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str = "svc", diffs: list | None = None) -> DriftResult:
    d = diffs or []
    return DriftResult(service=service, has_drift=bool(d), diffs=d)


def _diff(field: str, kind: str = "changed", expected: str = "a", actual: str = "b") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=expected, actual=actual)


# ---------------------------------------------------------------------------
# TestMaskRule
# ---------------------------------------------------------------------------

class TestMaskRule:
    def test_valid_rule_created(self):
        rule = MaskRule(pattern="password")
        assert rule.pattern == "password"
        assert rule.mask == "***"

    def test_custom_mask(self):
        rule = MaskRule(pattern="secret", mask="[REDACTED]")
        assert rule.mask == "[REDACTED]"

    def test_empty_pattern_raises(self):
        with pytest.raises(MaskerError, match="pattern"):
            MaskRule(pattern="")

    def test_whitespace_pattern_raises(self):
        with pytest.raises(MaskerError, match="pattern"):
            MaskRule(pattern="   ")

    def test_empty_mask_raises(self):
        with pytest.raises(MaskerError, match="mask"):
            MaskRule(pattern="token", mask="")

    def test_invalid_regex_raises(self):
        with pytest.raises(MaskerError, match="Invalid regex"):
            MaskRule(pattern="[invalid")

    def test_matches_exact(self):
        rule = MaskRule(pattern="^password$")
        assert rule.matches("password")
        assert not rule.matches("my_password")

    def test_matches_substring(self):
        rule = MaskRule(pattern="secret")
        assert rule.matches("api_secret_key")
        assert not rule.matches("token")


# ---------------------------------------------------------------------------
# TestMaskResults
# ---------------------------------------------------------------------------

class TestMaskResults:
    def test_none_results_raises(self):
        with pytest.raises(MaskerError):
            mask_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(MaskerError):
            mask_results([], None)

    def test_empty_results_returns_empty(self):
        assert mask_results([], []) == []

    def test_no_matching_rule_leaves_values(self):
        result = _make(diffs=[_diff("replicas", expected="3", actual="2")])
        rule = MaskRule(pattern="password")
        masked = mask_results([result], [rule])
        assert masked[0].diffs[0].expected == "3"
        assert masked[0].diffs[0].actual == "2"
        assert masked[0].masked_fields == []

    def test_matching_rule_masks_values(self):
        result = _make(diffs=[_diff("db_password", expected="hunter2", actual="letmein")])
        rule = MaskRule(pattern="password")
        masked = mask_results([result], [rule])
        assert masked[0].diffs[0].expected == "***"
        assert masked[0].diffs[0].actual == "***"
        assert "db_password" in masked[0].masked_fields

    def test_partial_mask_only_sensitive_fields(self):
        diffs = [
            _diff("replicas", expected="3", actual="2"),
            _diff("api_secret", expected="abc", actual="xyz"),
        ]
        result = _make(diffs=diffs)
        rule = MaskRule(pattern="secret")
        masked = mask_results([result], [rule])
        assert masked[0].diffs[0].expected == "3"
        assert masked[0].diffs[1].expected == "***"
        assert masked[0].masked_fields == ["api_secret"]

    def test_first_matching_rule_wins(self):
        result = _make(diffs=[_diff("token", expected="real", actual="fake")])
        rules = [MaskRule(pattern="token", mask="HIDE"), MaskRule(pattern="token", mask="GONE")]
        masked = mask_results([result], rules)
        assert masked[0].diffs[0].expected == "HIDE"

    def test_to_dict_contains_masked_fields(self):
        result = _make(diffs=[_diff("secret_key", expected="s", actual="t")])
        rule = MaskRule(pattern="secret")
        masked = mask_results([result], [rule])
        d = masked[0].to_dict()
        assert "masked_fields" in d
        assert d["masked_fields"] == ["secret_key"]

    def test_multiple_results_each_masked_independently(self):
        r1 = _make("svc-a", diffs=[_diff("password", expected="p1", actual="p2")])
        r2 = _make("svc-b", diffs=[_diff("replicas", expected="1", actual="2")])
        rule = MaskRule(pattern="password")
        masked = mask_results([r1, r2], [rule])
        assert masked[0].masked_fields == ["password"]
        assert masked[1].masked_fields == []
