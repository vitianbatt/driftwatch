"""Integration tests for correlator using the YAML fixture."""
from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.correlator import correlate

FIXTURE = Path(__file__).parent / "fixtures" / "sample_correlator_input.yaml"


def _load_fixture():
    with FIXTURE.open() as fh:
        raw = yaml.safe_load(fh)
    return [
        DriftResult(service=r["service"], drifted_fields=r.get("drifted_fields") or [])
        for r in raw
    ]


@pytest.fixture()
def report():
    return correlate(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_five_results_loaded(self):
        results = _load_fixture()
        assert len(results) == 5

    def test_metrics_service_has_no_drift(self):
        results = _load_fixture()
        metrics = next(r for r in results if r.service == "metrics-service")
        assert metrics.drifted_fields == []


class TestCorrelatorReport:
    def test_two_groups_found(self, report):
        assert report.total_groups() == 2

    def test_auth_and_billing_share_group(self, report):
        services_in_groups = [sorted(g.services) for g in report.groups]
        assert ["auth-service", "billing-service"] in services_in_groups

    def test_payment_and_gateway_share_group(self, report):
        services_in_groups = [sorted(g.services) for g in report.groups]
        assert ["gateway-service", "payment-service"] in services_in_groups

    def test_metrics_service_not_in_any_group(self, report):
        assert "metrics-service" not in report.services_in_any_group()

    def test_summary_mentions_group_count(self, report):
        assert "2 correlation group" in report.summary()

    def test_env_replicas_group_fields(self, report):
        env_group = next(
            g for g in report.groups if "env" in g.fields and "replicas" in g.fields
        )
        assert sorted(env_group.fields) == ["env", "replicas"]
