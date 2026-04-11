"""Integration tests for linker using the sample_dependency_map fixture."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.linker import DependencyMap, link_results

FIXTURE = Path(__file__).parent / "fixtures" / "sample_dependency_map.yaml"


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field: str) -> FieldDiff:
    return FieldDiff(field=field, expected="x", actual="y", kind="changed")


@pytest.fixture
def dep_map() -> DependencyMap:
    raw = yaml.safe_load(FIXTURE.read_text())
    return DependencyMap(deps=raw["deps"])


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_five_services_in_map(self, dep_map):
        assert len(dep_map.deps) == 5

    def test_api_service_has_two_deps(self, dep_map):
        assert len(dep_map.dependencies_of("api-service")) == 2

    def test_auth_service_has_no_deps(self, dep_map):
        assert dep_map.dependencies_of("auth-service") == []


class TestLinkIntegration:
    def test_no_drift_no_affected_by(self, dep_map):
        results = [_make("api-service"), _make("auth-service"), _make("db-service")]
        linked = link_results(results, dep_map)
        api_lr = next(lr for lr in linked if lr.result.service == "api-service")
        assert api_lr.affected_by == []

    def test_drifted_auth_propagates_to_api(self, dep_map):
        results = [
            _make("api-service"),
            _make("auth-service", [_diff("token_ttl")]),
            _make("db-service"),
        ]
        linked = link_results(results, dep_map)
        api_lr = next(lr for lr in linked if lr.result.service == "api-service")
        assert "auth-service" in api_lr.affected_by

    def test_drifted_db_propagates_to_worker(self, dep_map):
        results = [
            _make("worker-service"),
            _make("db-service", [_diff("pool_size")]),
        ]
        linked = link_results(results, dep_map)
        worker_lr = next(lr for lr in linked if lr.result.service == "worker-service")
        assert "db-service" in worker_lr.affected_by

    def test_clean_db_not_in_affected_by(self, dep_map):
        results = [_make("worker-service"), _make("db-service")]
        linked = link_results(results, dep_map)
        worker_lr = next(lr for lr in linked if lr.result.service == "worker-service")
        assert "db-service" not in worker_lr.affected_by

    def test_all_linked_results_have_to_dict(self, dep_map):
        results = [_make("api-service"), _make("db-service")]
        linked = link_results(results, dep_map)
        for lr in linked:
            d = lr.to_dict()
            assert "service" in d and "dependencies" in d
