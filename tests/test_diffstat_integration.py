"""Integration tests for diffstat using the YAML fixture."""
import pathlib

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.diffstat import build_diffstat

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_diffstat_input.yaml"


def _load_fixture():
    with open(FIXTURE) as fh:
        raw = yaml.safe_load(fh)
    return [
        DriftResult(service=entry["service"], drifted_fields=entry.get("drifted_fields") or [])
        for entry in raw
    ]


@pytest.fixture(scope="module")
def report():
    return build_diffstat(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self):
        results = _load_fixture()
        assert len(results) == 4


class TestDiffStatFromFixture:
    def test_total_results_is_four(self, report):
        assert report.total_results == 4

    def test_total_drifted_is_three(self, report):
        assert report.total_drifted == 3

    def test_timeout_appears_twice(self, report):
        assert report.stats["timeout"].occurrences == 2

    def test_max_connections_appears_twice(self, report):
        assert report.stats["max_connections"].occurrences == 2

    def test_retry_limit_appears_once(self, report):
        assert report.stats["retry_limit"].occurrences == 1

    def test_most_common_top_field_has_two_occurrences(self, report):
        top = report.most_common(1)
        assert top[0].occurrences == 2

    def test_user_service_not_in_any_stat(self, report):
        for stat in report.stats.values():
            assert "user-service" not in stat.services

    def test_summary_contains_drifted(self, report):
        assert "drifted" in report.summary()
