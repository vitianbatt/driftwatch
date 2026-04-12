"""Integration tests: load segmenter rules from the YAML fixture."""
from __future__ import annotations

import os

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.segmenter import SegmentRule, SegmenterError, segment_results

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "sample_segmenter_rules.yaml"
)


def _load_rules() -> list[SegmentRule]:
    with open(FIXTURE) as fh:
        data = yaml.safe_load(fh)
    return [SegmentRule(name=r["name"], pattern=r["pattern"]) for r in data["rules"]]


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=fields or [])


@pytest.fixture(scope="module")
def rules() -> list[SegmentRule]:
    return _load_rules()


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert os.path.isfile(FIXTURE)

    def test_three_rules_loaded(self, rules):
        assert len(rules) == 3

    def test_first_rule_name(self, rules):
        assert rules[0].name == "network"

    def test_first_rule_pattern(self, rules):
        assert rules[0].pattern == "net_*"

    def test_rule_names(self, rules):
        names = [r.name for r in rules]
        assert names == ["network", "database", "auth"]


class TestSegmentWithFixtureRules:
    def test_network_result_placed_correctly(self, rules):
        result = _make("svc-a", ["net_timeout", "net_retries"])
        report = segment_results([result], rules)
        assert report.size("network") == 1
        assert report.size("database") == 0

    def test_db_result_placed_correctly(self, rules):
        result = _make("svc-b", ["db_host"])
        report = segment_results([result], rules)
        assert report.size("database") == 1

    def test_auth_result_placed_correctly(self, rules):
        result = _make("svc-c", ["auth_token"])
        report = segment_results([result], rules)
        assert report.size("auth") == 1

    def test_unrecognised_field_goes_to_unmatched(self, rules):
        result = _make("svc-d", ["cache_ttl"])
        report = segment_results([result], rules)
        assert len(report.unmatched) == 1

    def test_mixed_results_total(self, rules):
        results = [
            _make("svc-a", ["net_timeout"]),
            _make("svc-b", ["db_port"]),
            _make("svc-c", ["auth_secret"]),
            _make("svc-d", ["unknown_field"]),
        ]
        report = segment_results(results, rules)
        assert report.total() == 4
        assert report.size("network") == 1
        assert report.size("database") == 1
        assert report.size("auth") == 1
        assert len(report.unmatched) == 1
