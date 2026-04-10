"""Integration tests for dispatcher using the sample fixture."""

from __future__ import annotations

import pathlib

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.dispatcher import DispatchRule, dispatch

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_dispatch_rules.yaml"

PREDICATE_MAP = {
    "has_drift": lambda r: bool(r.diffs),
    "no_drift": lambda r: not r.diffs,
    "always": lambda r: True,
}


def _load_rules(path: pathlib.Path) -> list[DispatchRule]:
    data = yaml.safe_load(path.read_text())
    rules = []
    for entry in data["rules"]:
        rules.append(
            DispatchRule(
                name=entry["name"],
                handler=lambda r: None,
                predicate=PREDICATE_MAP[entry["predicate"]],
            )
        )
    return rules


def _make(service: str, diffs: list | None = None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


@pytest.fixture()
def rules() -> list[DispatchRule]:
    return _load_rules(FIXTURE)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_three_rules_loaded(self, rules):
        assert len(rules) == 3

    def test_first_rule_name(self, rules):
        assert rules[0].name == "drift_alert"

    def test_second_rule_name(self, rules):
        assert rules[1].name == "clean_log"

    def test_third_rule_name(self, rules):
        assert rules[2].name == "catch_all"


class TestDispatchWithFixtureRules:
    def test_drifted_service_matches_drift_alert_and_catch_all(self, rules):
        result = _make("broken-svc", diffs=["env"])
        report = dispatch([result], rules)
        assert "broken-svc" in report.dispatched.get("drift_alert", [])
        assert "broken-svc" in report.dispatched.get("catch_all", [])
        assert "broken-svc" not in report.dispatched.get("clean_log", [])

    def test_clean_service_matches_clean_log_and_catch_all(self, rules):
        result = _make("ok-svc")
        report = dispatch([result], rules)
        assert "ok-svc" in report.dispatched.get("clean_log", [])
        assert "ok-svc" in report.dispatched.get("catch_all", [])
        assert "ok-svc" not in report.dispatched.get("drift_alert", [])

    def test_no_skipped_because_catch_all_always_matches(self, rules):
        results = [_make("svc-a"), _make("svc-b", ["x"])]
        report = dispatch(results, rules)
        assert report.skipped == []
