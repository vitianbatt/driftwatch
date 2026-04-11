"""Tests for driftwatch.partitioner."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.partitioner import (
    PartitionConfig,
    PartitionedReport,
    PartitionerError,
    partition_results,
)


def _make(service: str, env: str = "", diffs=None):
    spec = {"environment": env} if env else {}
    return DriftResult(service=service, diffs=diffs or [], spec=spec, live={})


# ---------------------------------------------------------------------------
# PartitionConfig
# ---------------------------------------------------------------------------

class TestPartitionConfig:
    def test_defaults_are_valid(self):
        cfg = PartitionConfig()
        assert cfg.env_field == "environment"
        assert cfg.default_partition == "unknown"

    def test_empty_env_field_raises(self):
        with pytest.raises(PartitionerError, match="env_field"):
            PartitionConfig(env_field="")

    def test_whitespace_env_field_raises(self):
        with pytest.raises(PartitionerError, match="env_field"):
            PartitionConfig(env_field="   ")

    def test_empty_default_partition_raises(self):
        with pytest.raises(PartitionerError, match="default_partition"):
            PartitionConfig(default_partition="")

    def test_custom_values_accepted(self):
        cfg = PartitionConfig(env_field="env", default_partition="other")
        assert cfg.env_field == "env"
        assert cfg.default_partition == "other"


# ---------------------------------------------------------------------------
# PartitionedReport
# ---------------------------------------------------------------------------

class TestPartitionedReport:
    def test_partition_names_sorted(self):
        r = PartitionedReport(partitions={"prod": [], "staging": [], "dev": []})
        assert r.partition_names() == ["dev", "prod", "staging"]

    def test_size_existing_partition(self):
        r = PartitionedReport(partitions={"prod": [_make("svc", "prod")]})
        assert r.size("prod") == 1

    def test_size_missing_partition_returns_zero(self):
        r = PartitionedReport(partitions={})
        assert r.size("prod") == 0

    def test_total_sums_all(self):
        r = PartitionedReport(
            partitions={"prod": [_make("a", "prod"), _make("b", "prod")], "dev": [_make("c", "dev")]}
        )
        assert r.total() == 3

    def test_summary_no_partitions(self):
        r = PartitionedReport()
        assert r.summary() == "No partitions."

    def test_summary_lists_partitions(self):
        r = PartitionedReport(partitions={"prod": [_make("a", "prod")], "dev": []})
        text = r.summary()
        assert "prod: 1 result(s)" in text
        assert "dev: 0 result(s)" in text


# ---------------------------------------------------------------------------
# partition_results
# ---------------------------------------------------------------------------

class TestPartitionResults:
    def test_none_raises(self):
        with pytest.raises(PartitionerError):
            partition_results(None)

    def test_empty_list_returns_empty_report(self):
        report = partition_results([])
        assert report.total() == 0

    def test_single_result_placed_in_correct_partition(self):
        result = _make("auth", "prod")
        report = partition_results([result])
        assert "prod" in report.partition_names()
        assert report.size("prod") == 1

    def test_missing_env_field_falls_back_to_default(self):
        result = DriftResult(service="svc", diffs=[], spec={}, live={})
        report = partition_results([result])
        assert "unknown" in report.partition_names()

    def test_custom_default_partition(self):
        result = DriftResult(service="svc", diffs=[], spec={}, live={})
        cfg = PartitionConfig(default_partition="unclassified")
        report = partition_results([result], config=cfg)
        assert "unclassified" in report.partition_names()

    def test_multiple_results_grouped_correctly(self):
        results = [
            _make("a", "prod"),
            _make("b", "staging"),
            _make("c", "prod"),
        ]
        report = partition_results(results)
        assert report.size("prod") == 2
        assert report.size("staging") == 1

    def test_custom_env_field(self):
        r = DriftResult(service="svc", diffs=[], spec={"region": "us-east"}, live={})
        cfg = PartitionConfig(env_field="region")
        report = partition_results([r], config=cfg)
        assert "us-east" in report.partition_names()
