"""Integration tests for curator using the fixture file."""
from __future__ import annotations

import pathlib

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.curator import curate

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_curator_input.yaml"


def _load_fixture() -> list[DriftResult]:
    data = yaml.safe_load(FIXTURE.read_text())
    results = []
    for entry in data:
        diffs = [
            FieldDiff(
                field=d["field"],
                expected=d["expected"],
                actual=d["actual"],
                diff_type=d["diff_type"],
            )
            for d in (entry.get("diffs") or [])
        ]
        results.append(DriftResult(service=entry["service"], diffs=diffs))
    return results


@pytest.fixture()
def report():
    return curate(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_five_raw_entries_loaded(self):
        raw = _load_fixture()
        assert len(raw) == 5


class TestCuratorIntegration:
    def test_three_unique_services_retained(self, report):
        assert len(report) == 3

    def test_two_duplicates_dropped(self, report):
        assert report.dropped == 2

    def test_auth_service_last_entry_wins(self, report):
        auth = next(r for r in report.results if r.service == "auth-service")
        # Last entry for auth-service has no diffs
        assert auth.diffs == []

    def test_payment_service_last_entry_wins(self, report):
        payment = next(r for r in report.results if r.service == "payment-service")
        # Last entry for payment-service has drift
        assert len(payment.diffs) == 1
        assert payment.diffs[0].field == "memory_limit"

    def test_inventory_service_present(self, report):
        names = report.service_names()
        assert "inventory-service" in names

    def test_summary_mentions_retained_count(self, report):
        assert "3 result" in report.summary()
