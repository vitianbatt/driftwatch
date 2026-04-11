"""Tests for driftwatch.router."""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.router import RouteRule, RoutedReport, RouterError, route_results


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# RouteRule
# ---------------------------------------------------------------------------

class TestRouteRule:
    def test_valid_rule_created(self):
        rule = RouteRule(destination="slack", pattern="auth-*")
        assert rule.destination == "slack"
        assert rule.pattern == "auth-*"

    def test_empty_destination_raises(self):
        with pytest.raises(RouterError, match="destination"):
            RouteRule(destination="", pattern="*")

    def test_whitespace_destination_raises(self):
        with pytest.raises(RouterError, match="destination"):
            RouteRule(destination="   ", pattern="*")

    def test_empty_pattern_raises(self):
        with pytest.raises(RouterError, match="pattern"):
            RouteRule(destination="ops", pattern="")

    def test_wildcard_matches_any(self):
        rule = RouteRule(destination="ops", pattern="*")
        assert rule.matches("anything")
        assert rule.matches("auth-service")

    def test_prefix_glob_matches(self):
        rule = RouteRule(destination="team-a", pattern="auth-*")
        assert rule.matches("auth-service")
        assert rule.matches("auth-v2")
        assert not rule.matches("billing-service")

    def test_exact_pattern_matches(self):
        rule = RouteRule(destination="team-b", pattern="billing")
        assert rule.matches("billing")
        assert not rule.matches("billing-v2")


# ---------------------------------------------------------------------------
# RoutedReport helpers
# ---------------------------------------------------------------------------

class TestRoutedReport:
    def test_destination_names_sorted(self):
        r = RoutedReport(routes={"ops": [], "dev": []})
        assert r.destination_names() == ["dev", "ops"]

    def test_size_missing_destination_returns_zero(self):
        r = RoutedReport()
        assert r.size("nowhere") == 0

    def test_total_includes_unrouted(self):
        r = RoutedReport(
            routes={"ops": [_make("a"), _make("b")]},
            unrouted=[_make("c")],
        )
        assert r.total() == 3

    def test_summary_no_results(self):
        assert RoutedReport().summary() == "no results"

    def test_summary_lists_destinations_and_unrouted(self):
        r = RoutedReport(
            routes={"ops": [_make("a")]},
            unrouted=[_make("z")],
        )
        assert "ops:1" in r.summary()
        assert "unrouted:1" in r.summary()


# ---------------------------------------------------------------------------
# route_results
# ---------------------------------------------------------------------------

def test_none_results_raises():
    with pytest.raises(RouterError):
        route_results(None, [])


def test_none_rules_raises():
    with pytest.raises(RouterError):
        route_results([], None)


def test_empty_results_returns_empty_report():
    report = route_results([], [RouteRule(destination="ops")])
    assert report.total() == 0


def test_single_rule_routes_all():
    results = [_make("svc-a"), _make("svc-b")]
    rules = [RouteRule(destination="ops", pattern="*")]
    report = route_results(results, rules)
    assert report.size("ops") == 2
    assert len(report.unrouted) == 0


def test_first_matching_rule_wins():
    results = [_make("auth-service")]
    rules = [
        RouteRule(destination="team-a", pattern="auth-*"),
        RouteRule(destination="team-b", pattern="*"),
    ]
    report = route_results(results, rules)
    assert report.size("team-a") == 1
    assert report.size("team-b") == 0


def test_unmatched_goes_to_unrouted():
    results = [_make("unknown-svc")]
    rules = [RouteRule(destination="ops", pattern="auth-*")]
    report = route_results(results, rules)
    assert len(report.unrouted) == 1
    assert report.size("ops") == 0


def test_allow_unrouted_false_raises():
    results = [_make("unknown-svc")]
    rules = [RouteRule(destination="ops", pattern="auth-*")]
    with pytest.raises(RouterError, match="no route matched"):
        route_results(results, rules, allow_unrouted=False)


def test_multiple_destinations():
    results = [_make("auth-v1"), _make("billing"), _make("auth-v2")]
    rules = [
        RouteRule(destination="auth-team", pattern="auth-*"),
        RouteRule(destination="billing-team", pattern="billing"),
    ]
    report = route_results(results, rules)
    assert report.size("auth-team") == 2
    assert report.size("billing-team") == 1
    assert len(report.unrouted) == 0
