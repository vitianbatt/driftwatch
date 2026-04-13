"""Tests for driftwatch.clusterer."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.clusterer import (
    Cluster,
    ClusteredReport,
    ClustererError,
    build_clusters,
)


def _diff(field: str, kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="a", actual="b")


def _make(service: str, *fields: str) -> DriftResult:
    diffs = [_diff(f) for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# Cluster dataclass
# ---------------------------------------------------------------------------

class TestCluster:
    def test_len_reflects_results(self):
        c = Cluster(label="c", common_fields=["x"], results=[_make("svc-a", "x")])
        assert len(c) == 1

    def test_service_names(self):
        c = Cluster(label="c", common_fields=["x"], results=[_make("svc-a", "x"), _make("svc-b", "x")])
        assert sorted(c.service_names()) == ["svc-a", "svc-b"]

    def test_to_dict_keys(self):
        c = Cluster(label="c", common_fields=["x"], results=[_make("svc-a", "x")])
        d = c.to_dict()
        assert set(d.keys()) == {"label", "common_fields", "services", "size"}

    def test_to_dict_size(self):
        c = Cluster(label="c", common_fields=["x"], results=[_make("a", "x"), _make("b", "x")])
        assert c.to_dict()["size"] == 2


# ---------------------------------------------------------------------------
# build_clusters
# ---------------------------------------------------------------------------

class TestBuildClusters:
    def test_none_raises(self):
        with pytest.raises(ClustererError):
            build_clusters(None)

    def test_zero_min_shared_raises(self):
        with pytest.raises(ClustererError):
            build_clusters([], min_shared_fields=0)

    def test_empty_list_returns_empty_report(self):
        report = build_clusters([])
        assert report.clusters == {}
        assert report.unclustered == []

    def test_clean_results_go_to_unclustered(self):
        r = _make("svc-a")
        report = build_clusters([r])
        assert r in report.unclustered
        assert report.clusters == {}

    def test_single_drift_no_shared_goes_unclustered(self):
        r = _make("svc-a", "timeout")
        report = build_clusters([r])
        assert r in report.unclustered

    def test_two_results_sharing_field_form_cluster(self):
        r1 = _make("svc-a", "timeout")
        r2 = _make("svc-b", "timeout")
        report = build_clusters([r1, r2])
        assert len(report.clusters) == 1
        cluster = next(iter(report.clusters.values()))
        assert "timeout" in cluster.common_fields
        assert "svc-a" in cluster.service_names()
        assert "svc-b" in cluster.service_names()

    def test_unrelated_fields_stay_unclustered(self):
        r1 = _make("svc-a", "timeout")
        r2 = _make("svc-b", "replicas")
        report = build_clusters([r1, r2])
        assert report.clusters == {}
        assert len(report.unclustered) == 2

    def test_total_sums_all(self):
        r1 = _make("svc-a", "timeout")
        r2 = _make("svc-b", "timeout")
        r3 = _make("svc-c")
        report = build_clusters([r1, r2, r3])
        assert report.total() == 3

    def test_summary_contains_cluster_name(self):
        r1 = _make("svc-a", "timeout")
        r2 = _make("svc-b", "timeout")
        report = build_clusters([r1, r2])
        s = report.summary()
        assert "cluster:timeout" in s

    def test_cluster_names_sorted(self):
        r1 = _make("svc-a", "alpha")
        r2 = _make("svc-b", "alpha")
        r3 = _make("svc-c", "beta")
        r4 = _make("svc-d", "beta")
        report = build_clusters([r1, r2, r3, r4])
        assert report.cluster_names() == sorted(report.cluster_names())
