"""Integration tests for aggregator using YAML fixture."""

import pytest
import yaml
from pathlib import Path

from driftwatch.comparator import DriftResult
from driftwatch.aggregator import aggregate, AggregateReport
from driftwatch.filter import Severity

FIXTURE = Path(__file__).parent / "fixtures" / "sample_aggregate_results.yaml"


def _load_fixture() -> list:
    with FIXTURE.open() as f:
        raw = yaml.safe_load(f)
    results = []
    for item in raw:
        changed = {k: tuple(v) for k, v in (item.get("changed_keys") or {}).items()}
        results.append(
            DriftResult(
                service=item["service"],
                missing_keys=item.get("missing_keys") or [],
                extra_keys=item.get("extra_keys") or [],
                changed_keys=changed,
            )
        )
    return results


@pytest.fixture
def report() -> AggregateReport:
    return aggregate(_load_fixture())


class TestFixtureAggregate:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_total_is_four(self, report):
        assert report.total_services == 4

    def test_two_clean_services(self, report):
        assert report.clean_services == 2

    def test_two_drifted_services(self, report):
        assert report.drifted_services == 2

    def test_drift_rate_is_half(self, report):
        assert report.drift_rate == 0.5

    def test_auth_service_is_clean(self, report):
        auth = next(s for s in report.summaries if s.service == "auth-service")
        assert auth.has_drift is False
        assert auth.drift_field_count == 0

    def test_payment_service_has_drift(self, report):
        pay = next(s for s in report.summaries if s.service == "payment-service")
        assert pay.has_drift is True
        assert pay.drift_field_count == 2

    def test_severity_counts_sum_to_total(self, report):
        assert sum(report.severity_counts.values()) == report.total_services
