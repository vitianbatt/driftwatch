"""Integration tests for resolver using the sample_owner_map.yaml fixture."""
import json
from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.resolver import OwnerMap, resolve_results, unowned
from driftwatch.resolver_cli import owner_map_from_yaml, results_from_json, run_resolver

FIXTURE = Path("tests/fixtures/sample_owner_map.yaml")


@pytest.fixture()
def owner_map() -> OwnerMap:
    return owner_map_from_yaml(str(FIXTURE))


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_owners_in_map(self, owner_map):
        assert len(owner_map.mappings) == 4

    def test_auth_service_owner(self, owner_map):
        assert owner_map.lookup("auth-service") == "team-platform"

    def test_billing_service_owner(self, owner_map):
        assert owner_map.lookup("billing-service") == "team-finance"

    def test_unknown_service_is_none(self, owner_map):
        assert owner_map.lookup("unknown-service") is None


class TestResolveWithFixture:
    def test_all_known_services_get_owners(self, owner_map):
        results = [
            DriftResult(service="auth-service", drift_fields=[]),
            DriftResult(service="billing-service", drift_fields=["timeout"]),
        ]
        resolved = resolve_results(results, owner_map)
        assert all(r.has_owner() for r in resolved)

    def test_unknown_service_has_no_owner(self, owner_map):
        results = [DriftResult(service="mystery-service", drift_fields=[])]
        resolved = resolve_results(results, owner_map)
        assert not resolved[0].has_owner()

    def test_unowned_returns_only_unmapped(self, owner_map):
        results = [
            DriftResult(service="auth-service", drift_fields=[]),
            DriftResult(service="mystery-service", drift_fields=["replicas"]),
        ]
        resolved = resolve_results(results, owner_map)
        missing = unowned(resolved)
        assert len(missing) == 1
        assert missing[0].result.service == "mystery-service"


class TestRunResolver:
    def test_run_resolver_returns_json_array(self, tmp_path):
        raw = json.dumps([
            {"service": "auth-service", "drift_fields": []},
            {"service": "billing-service", "drift_fields": ["cpu"]},
        ])
        out = run_resolver(raw, str(FIXTURE))
        parsed = json.loads(out)
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    def test_run_resolver_show_unowned_filters(self):
        raw = json.dumps([
            {"service": "auth-service", "drift_fields": []},
            {"service": "orphan-service", "drift_fields": ["memory"]},
        ])
        out = run_resolver(raw, str(FIXTURE), show_unowned=True)
        parsed = json.loads(out)
        assert len(parsed) == 1
        assert parsed[0]["service"] == "orphan-service"
