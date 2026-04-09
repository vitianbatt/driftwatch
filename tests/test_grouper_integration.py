"""Integration tests: load fixture results and group them."""
from __future__ import annotations

import pathlib

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.grouper import GroupBy, group_results

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_grouper_results.yaml"


def _load_fixture() -> list[DriftResult]:
    raw = yaml.safe_load(FIXTURE.read_text())
    results = []
    for item in raw:
        diffs = [str(d) for d in (item.get("diffs") or [])]
        results.append(DriftResult(service=item["service"], diffs=diffs))
    return results


@pytest.fixture(scope="module")
def results() -> list[DriftResult]:
    return _load_fixture()


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self, results):
        assert len(results) == 4


class TestGroupByServiceIntegration:
    def test_two_distinct_services(self, results):
        report = group_results(results, GroupBy.SERVICE)
        assert len(report.group_names()) == 3

    def test_auth_service_has_two_entries(self, results):
        report = group_results(results, GroupBy.SERVICE)
        assert report.size("auth-service") == 2

    def test_billing_service_has_one_entry(self, results):
        report = group_results(results, GroupBy.SERVICE)
        assert report.size("billing-service") == 1

    def test_total_matches_fixture(self, results):
        report = group_results(results, GroupBy.SERVICE)
        assert report.total() == 4


class TestGroupBySeverityIntegration:
    def test_low_bucket_exists(self, results):
        report = group_results(results, GroupBy.SEVERITY)
        assert "low" in report.group_names()

    def test_high_bucket_exists(self, results):
        # gateway-service has 3 diffs → high
        report = group_results(results, GroupBy.SEVERITY)
        assert "high" in report.group_names()

    def test_summary_mentions_dimension(self, results):
        report = group_results(results, GroupBy.SEVERITY)
        assert "severity" in report.summary()


class TestGroupByTagIntegration:
    def test_tagged_services_grouped_correctly(self, results):
        tag_map = {
            "auth-service": "identity",
            "billing-service": "finance",
            "gateway-service": "infra",
        }
        report = group_results(results, GroupBy.TAG, tag_map=tag_map)
        assert report.size("identity") == 2
        assert report.size("finance") == 1
        assert report.size("infra") == 1

    def test_untagged_bucket_absent_when_all_known(self, results):
        tag_map = {
            "auth-service": "identity",
            "billing-service": "finance",
            "gateway-service": "infra",
        }
        report = group_results(results, GroupBy.TAG, tag_map=tag_map)
        assert "untagged" not in report.group_names()
