"""Integration tests: load classification rules from fixture and classify results."""

from __future__ import annotations

import pathlib

import pytest
import yaml

from driftwatch.classifier import ClassificationRule, ClassifierError, classify_results
from driftwatch.comparator import DriftResult

FIXTURE = pathlib.Path("tests/fixtures/sample_classification_rules.yaml")


def _load_rules():
    data = yaml.safe_load(FIXTURE.read_text())
    return [
        ClassificationRule(category=r["category"], pattern=r["pattern"])
        for r in data["rules"]
    ]


def _make(service: str, missing=(), extra=()) -> DriftResult:
    return DriftResult(service=service, missing_keys=list(missing), extra_keys=list(extra))


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_rules_loaded(self):
        rules = _load_rules()
        assert len(rules) == 4

    def test_first_rule_is_network(self):
        rules = _load_rules()
        assert rules[0].category == "network"

    def test_all_rules_have_patterns(self):
        rules = _load_rules()
        for rule in rules:
            assert rule.pattern


class TestClassifyWithFixture:
    def test_network_field_classified(self):
        rules = _load_rules()
        result = _make("svc", missing=["port_grpc"])
        classified = classify_results([result], rules)
        assert "network" in classified[0].categories

    def test_auth_field_classified(self):
        rules = _load_rules()
        result = _make("svc", missing=["auth_token"])
        classified = classify_results([result], rules)
        assert "auth" in classified[0].categories

    def test_storage_field_classified(self):
        rules = _load_rules()
        result = _make("svc", missing=["db_host"])
        classified = classify_results([result], rules)
        assert "storage" in classified[0].categories

    def test_observability_field_classified(self):
        rules = _load_rules()
        result = _make("svc", missing=["log_level"])
        classified = classify_results([result], rules)
        assert "observability" in classified[0].categories

    def test_unknown_field_unclassified(self):
        rules = _load_rules()
        result = _make("svc", missing=["foobar_xyz"])
        classified = classify_results([result], rules)
        assert "foobar_xyz" in classified[0].unclassified_fields
        assert classified[0].categories == []
