"""Integration tests: load suppression rules from YAML fixture and apply them."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.suppressor import apply_suppressions, load_rules_from_dicts

FIXTURE = Path(__file__).parent / "fixtures" / "sample_suppression_rules.yaml"


@pytest.fixture(scope="module")
def rules():
    data = yaml.safe_load(FIXTURE.read_text())
    return load_rules_from_dicts(data["rules"])


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_three_rules_loaded(self, rules):
        assert len(rules) == 3

    def test_first_rule_service(self, rules):
        assert rules[0].service == "auth-service"

    def test_first_rule_has_reason(self, rules):
        assert rules[0].reason == "replicas managed by autoscaler"

    def test_second_rule_has_two_fields(self, rules):
        assert len(rules[1].fields) == 2

    def test_third_rule_has_no_fields(self, rules):
        assert rules[2].fields == []


class TestApplyFixtureRules:
    def test_auth_replicas_suppressed(self, rules):
        result = DriftResult(service="auth-service", drifted_fields=["replicas", "image"])
        out = apply_suppressions([result], rules)
        assert "replicas" not in out[0].drifted_fields
        assert "image" in out[0].drifted_fields

    def test_legacy_service_image_suppressed(self, rules):
        result = DriftResult(service="legacy-worker", drifted_fields=["image", "memory"])
        out = apply_suppressions([result], rules)
        assert "image" not in out[0].drifted_fields
        assert "memory" in out[0].drifted_fields

    def test_canary_fully_suppressed(self, rules):
        result = DriftResult(service="canary-service", drifted_fields=["replicas", "image"])
        out = apply_suppressions([result], rules)
        assert out[0].drifted_fields == []

    def test_unrelated_service_untouched(self, rules):
        result = DriftResult(service="payment-service", drifted_fields=["replicas"])
        out = apply_suppressions([result], rules)
        assert out[0].drifted_fields == ["replicas"]
