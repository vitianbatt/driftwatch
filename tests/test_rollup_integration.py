"""Integration tests for rollup using the YAML fixture."""
import pathlib
import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.rollup import build_rollup
from driftwatch.filter import Severity

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_rollup_results.yaml"


@pytest.fixture(scope="module")
def rollup_report():
    raw = yaml.safe_load(FIXTURE.read_text())
    results = [
        DriftResult(service=r["service"], diffs=r.get("diffs") or {})
        for r in raw["results"]
    ]
    return build_rollup(results)


class TestFixtureRollup:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_total_is_four(self, rollup_report):
        assert rollup_report.total == 4

    def test_two_clean_services(self, rollup_report):
        assert rollup_report.clean == 2

    def test_two_drifted_services(self, rollup_report):
        assert rollup_report.drifted == 2

    def test_drifted_services_names(self, rollup_report):
        assert "payment-service" in rollup_report.drifted_services
        assert "gateway-service" in rollup_report.drifted_services

    def test_clean_services_not_in_drifted(self, rollup_report):
        assert "auth-service" not in rollup_report.drifted_services
        assert "notification-service" not in rollup_report.drifted_services

    def test_has_any_drift_true(self, rollup_report):
        assert rollup_report.has_any_drift() is True

    def test_by_severity_sums_to_total(self, rollup_report):
        total = sum(rollup_report.by_severity.values())
        assert total == rollup_report.total

    def test_summary_string_non_empty(self, rollup_report):
        s = rollup_report.summary()
        assert len(s) > 0
        assert "payment-service" in s
