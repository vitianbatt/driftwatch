"""Integration tests for driftwatch.router using fixture data."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.router import RouteRule, RouterError, route_results

FIXTURE = Path(__file__).parent / "fixtures" / "sample_route_rules.yaml"


def _load_rules() -> list[RouteRule]:
    data = yaml.safe_load(FIXTURE.read_text())
    return [
        RouteRule(destination=r["destination"], pattern=r["pattern"])
        for r in data["rules"]
    ]


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------

class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_three_rules_loaded(self):
        rules = _load_rules()
        assert len(rules) == 3

    def test_first_rule_destination(self):
        rules = _load_rules()
        assert rules[0].destination == "auth-team"

    def test_first_rule_pattern(self):
        rules = _load_rules()
        assert rules[0].pattern == "auth-*"

    def test_last_rule_is_catchall(self):
        rules = _load_rules()
        assert rules[-1].pattern == "*"
        assert rules[-1].destination == "ops"


# ---------------------------------------------------------------------------
# Routing with fixture rules
# ---------------------------------------------------------------------------

@pytest.fixture()
def rules() -> list[RouteRule]:
    return _load_rules()


def test_auth_service_routes_to_auth_team(rules):
    results = [_make("auth-service"), _make("auth-v2")]
    report = route_results(results, rules)
    assert report.size("auth-team") == 2


def test_billing_service_routes_to_billing_team(rules):
    results = [_make("billing-api"), _make("billing-worker")]
    report = route_results(results, rules)
    assert report.size("billing-team") == 2


def test_unknown_service_falls_through_to_ops(rules):
    results = [_make("metrics-service")]
    report = route_results(results, rules)
    assert report.size("ops") == 1
    assert len(report.unrouted) == 0


def test_mixed_results_all_routed(rules):
    results = [
        _make("auth-service"),
        _make("billing-api"),
        _make("metrics-service"),
    ]
    report = route_results(results, rules)
    assert report.total() == 3
    assert len(report.unrouted) == 0
    assert report.size("auth-team") == 1
    assert report.size("billing-team") == 1
    assert report.size("ops") == 1
