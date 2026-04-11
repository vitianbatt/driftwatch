"""Tests for driftwatch.matcher."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.matcher import (
    MatcherError,
    MatchReport,
    MatchRule,
    match_results,
)


def _make(service: str, drifted: bool = False) -> DriftResult:
    fields = ["env"] if drifted else []
    return DriftResult(service=service, drifted_fields=fields)


# ---------------------------------------------------------------------------
# MatchRule
# ---------------------------------------------------------------------------

class TestMatchRule:
    def test_valid_glob_created(self):
        rule = MatchRule(pattern="auth-*")
        assert rule.pattern == "auth-*"

    def test_empty_pattern_raises(self):
        with pytest.raises(MatcherError):
            MatchRule(pattern="")

    def test_whitespace_pattern_raises(self):
        with pytest.raises(MatcherError):
            MatchRule(pattern="   ")

    def test_invalid_regex_raises(self):
        with pytest.raises(MatcherError):
            MatchRule(pattern="[unclosed", use_regex=True)

    def test_glob_match_true(self):
        rule = MatchRule(pattern="auth-*")
        assert rule.matches("auth-service") is True

    def test_glob_match_false(self):
        rule = MatchRule(pattern="auth-*")
        assert rule.matches("payment-service") is False

    def test_regex_match_true(self):
        rule = MatchRule(pattern=r"auth-\w+", use_regex=True)
        assert rule.matches("auth-service") is True

    def test_regex_match_false(self):
        rule = MatchRule(pattern=r"auth-\w+", use_regex=True)
        assert rule.matches("payment-service") is False


# ---------------------------------------------------------------------------
# match_results
# ---------------------------------------------------------------------------

class TestMatchResults:
    def test_none_results_raises(self):
        with pytest.raises(MatcherError):
            match_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(MatcherError):
            match_results([], None)

    def test_empty_results_returns_empty_report(self):
        report = match_results([], [MatchRule("auth-*")])
        assert report.total_matched == 0
        assert report.unmatched == []

    def test_matching_service_goes_to_matched(self):
        results = [_make("auth-service"), _make("payment-service")]
        rules = [MatchRule("auth-*")]
        report = match_results(results, rules)
        assert report.total_matched == 1
        assert report.matched[0].service == "auth-service"

    def test_unmatched_service_goes_to_unmatched(self):
        results = [_make("payment-service")]
        rules = [MatchRule("auth-*")]
        report = match_results(results, rules)
        assert len(report.unmatched) == 1

    def test_no_rules_all_unmatched(self):
        results = [_make("auth-service")]
        report = match_results(results, [])
        assert report.total_matched == 0
        assert len(report.unmatched) == 1

    def test_require_all_must_satisfy_every_rule(self):
        results = [_make("auth-service"), _make("auth-db")]
        rules = [MatchRule("auth-*"), MatchRule("*-service")]
        report = match_results(results, rules, require_all=True)
        assert report.total_matched == 1
        assert report.matched[0].service == "auth-service"

    def test_summary_string_format(self):
        results = [_make("auth-service"), _make("payment-service")]
        rules = [MatchRule("auth-*")]
        report = match_results(results, rules)
        assert "matched=1" in report.summary()
        assert "unmatched=1" in report.summary()
