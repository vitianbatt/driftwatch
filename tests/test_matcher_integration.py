"""Integration tests for matcher using YAML fixture."""
import pathlib

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.matcher import MatchRule, MatcherError, match_results

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_match_rules.yaml"


def _load_rules():
    with FIXTURE.open() as fh:
        data = yaml.safe_load(fh)
    return [
        MatchRule(pattern=r["pattern"], use_regex=r.get("use_regex", False))
        for r in data["rules"]
    ]


def _make(service: str) -> DriftResult:
    return DriftResult(service=service, drifted_fields=[])


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_three_rules_loaded(self):
        rules = _load_rules()
        assert len(rules) == 3

    def test_first_rule_is_glob(self):
        rules = _load_rules()
        assert rules[0].use_regex is False
        assert rules[0].pattern == "auth-*"

    def test_third_rule_is_regex(self):
        rules = _load_rules()
        assert rules[2].use_regex is True


class TestMatchWithFixtureRules:
    def test_auth_service_matched_by_first_rule(self):
        rules = _load_rules()
        results = [_make("auth-service")]
        report = match_results(results, rules[:1])
        assert report.total_matched == 1

    def test_payment_service_matched_by_second_rule(self):
        rules = _load_rules()
        results = [_make("payment-api")]
        report = match_results(results, [rules[1]])
        assert report.total_matched == 1

    def test_worker_matched_by_regex_rule(self):
        rules = _load_rules()
        results = [_make("email-worker")]
        report = match_results(results, [rules[2]])
        assert report.total_matched == 1

    def test_unknown_service_unmatched_by_all_rules(self):
        rules = _load_rules()
        results = [_make("unknown-svc")]
        report = match_results(results, rules)
        assert report.total_matched == 0
        assert len(report.unmatched) == 1
