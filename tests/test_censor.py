"""Tests for driftwatch.censor."""
import pytest

from driftwatch.censor import (
    CensorError,
    CensorRule,
    CensoredResult,
    censor_results,
)
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _diff(field: str, kind: str = "changed", expected: str = "a", actual: str = "b") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=expected, actual=actual)


def _make(service: str, *diffs: FieldDiff) -> DriftResult:
    return DriftResult(service=service, diffs=list(diffs))


# ---------------------------------------------------------------------------
# CensorRule
# ---------------------------------------------------------------------------

class TestCensorRule:
    def test_valid_rule_created(self):
        rule = CensorRule(field_name="password")
        assert rule.field_name == "password"
        assert rule.placeholder == "<censored>"

    def test_custom_placeholder_accepted(self):
        rule = CensorRule(field_name="token", placeholder="***")
        assert rule.placeholder == "***"

    def test_empty_field_name_raises(self):
        with pytest.raises(CensorError):
            CensorRule(field_name="")

    def test_whitespace_field_name_raises(self):
        with pytest.raises(CensorError):
            CensorRule(field_name="   ")

    def test_empty_placeholder_raises(self):
        with pytest.raises(CensorError):
            CensorRule(field_name="secret", placeholder="")

    def test_matches_correct_field(self):
        rule = CensorRule(field_name="api_key")
        assert rule.matches(_diff("api_key")) is True

    def test_does_not_match_other_field(self):
        rule = CensorRule(field_name="api_key")
        assert rule.matches(_diff("replicas")) is False


# ---------------------------------------------------------------------------
# CensoredResult
# ---------------------------------------------------------------------------

class TestCensoredResult:
    def test_has_drift_false_when_empty(self):
        r = CensoredResult(service="svc")
        assert r.has_drift() is False

    def test_has_drift_true_when_diffs(self):
        r = CensoredResult(service="svc", diffs=[_diff("x")])
        assert r.has_drift() is True

    def test_to_dict_contains_expected_keys(self):
        r = CensoredResult(service="svc", diffs=[_diff("x")], censored_fields=["x"])
        d = r.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "censored_fields", "diffs"}

    def test_to_dict_censored_fields_sorted(self):
        r = CensoredResult(service="svc", censored_fields=["z", "a"])
        assert r.to_dict()["censored_fields"] == ["a", "z"]


# ---------------------------------------------------------------------------
# censor_results
# ---------------------------------------------------------------------------

class TestCensorResults:
    def test_none_results_raises(self):
        with pytest.raises(CensorError):
            censor_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(CensorError):
            censor_results([], None)

    def test_empty_results_returns_empty(self):
        assert censor_results([], []) == []

    def test_no_matching_rules_leaves_diffs_unchanged(self):
        result = _make("svc", _diff("replicas", expected="2", actual="3"))
        censored = censor_results([result], [CensorRule(field_name="password")])
        assert len(censored) == 1
        assert censored[0].diffs[0].expected == "2"
        assert censored[0].censored_fields == []

    def test_matching_rule_replaces_values(self):
        result = _make("svc", _diff("password", expected="hunter2", actual="secret"))
        censored = censor_results([result], [CensorRule(field_name="password")])
        d = censored[0].diffs[0]
        assert d.expected == "<censored>"
        assert d.actual == "<censored>"

    def test_censored_field_recorded(self):
        result = _make("svc", _diff("api_key", expected="abc", actual="xyz"))
        censored = censor_results([result], [CensorRule(field_name="api_key")])
        assert "api_key" in censored[0].censored_fields

    def test_global_placeholder_override(self):
        result = _make("svc", _diff("token", expected="t1", actual="t2"))
        censored = censor_results(
            [result], [CensorRule(field_name="token")], placeholder="REDACTED"
        )
        assert censored[0].diffs[0].expected == "REDACTED"

    def test_unmatched_diffs_preserved_alongside_censored(self):
        result = _make(
            "svc",
            _diff("replicas", expected="1", actual="2"),
            _diff("secret", expected="old", actual="new"),
        )
        censored = censor_results([result], [CensorRule(field_name="secret")])
        assert censored[0].diffs[0].field == "replicas"
        assert censored[0].diffs[0].expected == "1"
        assert censored[0].diffs[1].expected == "<censored>"

    def test_multiple_results_processed(self):
        results = [
            _make("svc-a", _diff("password")),
            _make("svc-b", _diff("replicas")),
        ]
        censored = censor_results(results, [CensorRule(field_name="password")])
        assert len(censored) == 2
        assert censored[0].censored_fields == ["password"]
        assert censored[1].censored_fields == []
