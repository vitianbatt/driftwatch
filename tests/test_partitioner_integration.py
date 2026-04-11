"""Integration tests for partitioner using the fixture file."""
import yaml
import pytest
from pathlib import Path

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.partitioner import PartitionConfig, partition_results

FIXTURE = Path(__file__).parent / "fixtures" / "sample_partitioner_input.yaml"


def _load_fixture():
    with FIXTURE.open() as fh:
        raw = yaml.safe_load(fh)
    results = []
    for entry in raw:
        diffs = [
            FieldDiff(field=d["field"], expected=d["expected"], actual=d["actual"], kind=d["kind"])
            for d in (entry.get("diffs") or [])
        ]
        results.append(
            DriftResult(
                service=entry["service"],
                diffs=diffs,
                spec=entry.get("spec", {}),
                live=entry.get("live", {}),
            )
        )
    return results


@pytest.fixture(scope="module")
def results():
    return _load_fixture()


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self, results):
        assert len(results) == 4


class TestPartitionerIntegration:
    def test_two_prod_services(self, results):
        report = partition_results(results)
        assert report.size("prod") == 2

    def test_one_staging_service(self, results):
        report = partition_results(results)
        assert report.size("staging") == 1

    def test_legacy_service_in_unknown(self, results):
        report = partition_results(results)
        assert report.size("unknown") == 1

    def test_total_matches_input(self, results):
        report = partition_results(results)
        assert report.total() == len(results)

    def test_summary_contains_prod(self, results):
        report = partition_results(results)
        assert "prod" in report.summary()

    def test_custom_default_relabels_unknown(self, results):
        cfg = PartitionConfig(default_partition="no-env")
        report = partition_results(results, config=cfg)
        assert "no-env" in report.partition_names()
        assert "unknown" not in report.partition_names()
