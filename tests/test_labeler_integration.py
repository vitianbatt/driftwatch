"""Integration tests for labeler using the sample_label_map.yaml fixture."""

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.labeler import filter_by_label, label_results

FIXTURE = Path(__file__).parent / "fixtures" / "sample_label_map.yaml"


def _make(service: str, drifted: list[str] | None = None) -> DriftResult:
    fields = drifted or []
    return DriftResult(service=service, has_drift=bool(fields), drifted_fields=fields)


@pytest.fixture()
def label_map() -> dict:
    with FIXTURE.open() as fh:
        return yaml.safe_load(fh)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_services_in_map(self, label_map):
        assert len(label_map) == 4

    def test_auth_service_present(self, label_map):
        assert "auth-service" in label_map

    def test_auth_env_is_production(self, label_map):
        assert label_map["auth-service"]["env"] == "production"

    def test_gateway_team_is_platform(self, label_map):
        assert label_map["gateway-service"]["team"] == "platform"


class TestLabelIntegration:
    @pytest.fixture()
    def results(self):
        return [
            _make("auth-service", ["replicas"]),
            _make("billing-service"),
            _make("gateway-service", ["timeout"]),
            _make("worker-service"),
        ]

    def test_all_results_labeled(self, results, label_map):
        labeled = label_results(results, label_map)
        assert len(labeled) == 4

    def test_production_filter_returns_two(self, results, label_map):
        labeled = label_results(results, label_map)
        prod = filter_by_label(labeled, "env", "production")
        assert len(prod) == 2

    def test_staging_filter_returns_two(self, results, label_map):
        labeled = label_results(results, label_map)
        staging = filter_by_label(labeled, "env", "staging")
        services = [lr.result.service for lr in staging]
        assert "gateway-service" in services
        assert "worker-service" in services

    def test_platform_team_filter(self, results, label_map):
        labeled = label_results(results, label_map)
        platform = filter_by_label(labeled, "team", "platform")
        assert len(platform) == 2

    def test_drift_preserved_after_labeling(self, results, label_map):
        labeled = label_results(results, label_map)
        auth_lr = next(lr for lr in labeled if lr.result.service == "auth-service")
        assert auth_lr.result.has_drift is True
        assert "replicas" in auth_lr.result.drifted_fields
