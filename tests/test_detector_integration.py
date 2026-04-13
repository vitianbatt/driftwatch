"""Integration tests for detector using the sample fixture."""
import pathlib

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.detector import detect_changes, DetectionReport

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_detector_input.yaml"


def _results_from_list(raw: list) -> list:
    return [
        DriftResult(service=r["service"], drifted_fields=r.get("drifted_fields") or [])
        for r in raw
    ]


@pytest.fixture(scope="module")
def report() -> DetectionReport:
    data = yaml.safe_load(FIXTURE.read_text())
    previous = _results_from_list(data["previous"])
    current = _results_from_list(data["current"])
    return detect_changes(previous, current)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_services_in_report(self, report):
        assert len(report.changes) == 4

    def test_services_sorted(self, report):
        names = [c.service for c in report.changes]
        assert names == sorted(names)

    def test_auth_service_has_appeared_and_disappeared(self, report):
        auth = next(c for c in report.changes if c.service == "auth-service")
        assert "log_level" in auth.appeared
        assert "memory_limit" in auth.disappeared

    def test_payment_service_timeout_disappeared(self, report):
        payment = next(c for c in report.changes if c.service == "payment-service")
        assert "timeout" in payment.disappeared
        assert payment.appeared == []

    def test_stable_service_no_change(self, report):
        stable = next(c for c in report.changes if c.service == "stable-service")
        assert not stable.has_change()

    def test_new_service_all_appeared(self, report):
        new_svc = next(c for c in report.changes if c.service == "new-service")
        assert "image_tag" in new_svc.appeared
        assert new_svc.disappeared == []

    def test_any_changes_true(self, report):
        assert report.any_changes() is True

    def test_summary_mentions_changed_services(self, report):
        s = report.summary()
        assert "auth-service" in s
        assert "payment-service" in s
