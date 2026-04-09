"""Integration tests for tagging using fixture data."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.tagging import filter_by_tag, group_by_tag, tag_results

FIXTURE = Path(__file__).parent / "fixtures" / "sample_tag_map.yaml"


def _make(service: str, n_drifted: int = 0) -> DriftResult:
    fields = [f"field_{i}" for i in range(n_drifted)]
    return DriftResult(
        service=service,
        drifted_fields=fields,
        missing_keys=[],
        extra_keys=[],
    )


@pytest.fixture()
def tag_map():
    return yaml.safe_load(FIXTURE.read_text())


@pytest.fixture()
def results():
    return [
        _make("auth-service", 2),
        _make("payment-service", 0),
        _make("notification-service", 1),
        _make("analytics-service", 0),
        _make("unknown-service", 3),
    ]


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_services_in_map(self, tag_map):
        assert len(tag_map) == 4

    def test_auth_has_critical_tag(self, tag_map):
        assert "critical" in tag_map["auth-service"]

    def test_payment_has_pci_tag(self, tag_map):
        assert "pci" in tag_map["payment-service"]


class TestTaggingIntegration:
    def test_all_results_tagged(self, results, tag_map):
        tagged = tag_results(results, tag_map)
        assert len(tagged) == 5

    def test_unknown_service_has_no_tags(self, results, tag_map):
        tagged = tag_results(results, tag_map)
        unknown = next(t for t in tagged if t.result.service == "unknown-service")
        assert unknown.tags == []

    def test_filter_prod_returns_three(self, results, tag_map):
        tagged = tag_results(results, tag_map)
        prod = filter_by_tag(tagged, "prod")
        assert len(prod) == 3

    def test_filter_pci_returns_one(self, results, tag_map):
        tagged = tag_results(results, tag_map)
        pci = filter_by_tag(tagged, "pci")
        assert len(pci) == 1
        assert pci[0].result.service == "payment-service"

    def test_group_by_tag_critical_has_two(self, results, tag_map):
        tagged = tag_results(results, tag_map)
        groups = group_by_tag(tagged)
        assert len(groups["critical"]) == 2

    def test_group_by_tag_untagged_has_one(self, results, tag_map):
        tagged = tag_results(results, tag_map)
        groups = group_by_tag(tagged)
        assert len(groups.get("", [])) == 1
