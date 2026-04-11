"""Integration tests: load real fixture and run redaction end-to-end."""

from __future__ import annotations

import os
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.redactor import redact_results
from driftwatch.redactor_cli import rules_from_yaml, run_redactor

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "sample_redact_rules.yaml")


@pytest.fixture()
def rules():
    return rules_from_yaml(FIXTURE)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert os.path.isfile(FIXTURE)

    def test_three_rules_loaded(self, rules):
        assert len(rules) == 3

    def test_first_rule_pattern(self, rules):
        assert rules[0].pattern == "password"

    def test_second_rule_custom_mask(self, rules):
        assert rules[1].mask == "[hidden]"

    def test_third_rule_pattern(self, rules):
        assert rules[2].pattern == "token"


class TestRedactionWithFixture:
    def _make(self, service: str, fields: list) -> DriftResult:
        diffs = [
            FieldDiff(field=f, kind="changed", expected="old_val", actual="new_val")
            for f in fields
        ]
        return DriftResult(service=service, diffs=diffs)

    def test_password_field_redacted(self, rules):
        result = self._make("auth-svc", ["db_password"])
        out = redact_results([result], rules)
        assert out[0].diffs[0].expected == "***REDACTED***"
        assert "db_password" in out[0].redacted_fields

    def test_token_field_redacted(self, rules):
        result = self._make("api-svc", ["api_token"])
        out = redact_results([result], rules)
        assert out[0].diffs[0].actual == "***REDACTED***"

    def test_secret_field_uses_custom_mask(self, rules):
        result = self._make("payment-svc", ["stripe_secret"])
        out = redact_results([result], rules)
        assert out[0].diffs[0].expected == "[hidden]"

    def test_non_sensitive_field_unchanged(self, rules):
        result = self._make("svc", ["replicas"])
        out = redact_results([result], rules)
        assert out[0].diffs[0].expected == "old_val"
        assert out[0].redacted_fields == []

    def test_run_redactor_returns_json_string(self, tmp_path):
        import json
        results_json = json.dumps([
            {"service": "svc", "diffs": [{"field": "db_password", "kind": "changed",
                                           "expected": "abc", "actual": "xyz"}]}
        ])
        output = run_redactor(FIXTURE, results_json)
        data = json.loads(output)
        assert data[0]["redacted_fields"] == ["db_password"]
