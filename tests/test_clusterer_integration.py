"""Integration tests for clusterer using the fixture file."""
from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.clusterer import build_clusters

FIXTURE = Path(__file__).parent / "fixtures" / "sample_clusterer_input.yaml"


def _load_fixture():
    raw = yaml.safe_load(FIXTURE.read_text())
    results = []
    for entry in raw:
        diffs = [
            FieldDiff(
                field=d["field"],
                kind=d["kind"],
                expected=d["expected"],
                actual=d["actual"],
            )
            for d in (entry.get("diffs") or [])
        ]
        results.append(DriftResult(service=entry["service"], diffs=diffs))
    return results


@pytest.fixture
def report():
    return build_clusters(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self):
        results = _load_fixture()
        assert len(results) == 4

    def test_total_in_report_is_four(self, report):
        assert report.total() == 4

    def test_timeout_cluster_exists(self, report):
        assert "cluster:timeout" in report.clusters

    def test_timeout_cluster_has_two_services(self, report):
        cluster = report.clusters["cluster:timeout"]
        assert len(cluster) == 2

    def test_replicas_cluster_exists(self, report):
        assert "cluster:replicas" in report.clusters

    def test_inventory_service_is_unclustered(self, report):
        unclustered_names = [r.service for r in report.unclustered]
        assert "inventory-service" in unclustered_names

    def test_summary_is_string(self, report):
        assert isinstance(report.summary(), str)
