"""Tests for driftwatch/cataloger.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.cataloger import (
    CatalogerError,
    CatalogEntry,
    CatalogReport,
    build_catalog,
)


def _make(service: str, drifted_fields=None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=drifted_fields or [])


# ---------------------------------------------------------------------------
# CatalogEntry
# ---------------------------------------------------------------------------

class TestCatalogEntry:
    def test_has_drift_false_when_no_drifted_fields(self):
        entry = CatalogEntry(service="svc", results=[_make("svc")])
        assert entry.has_drift is False

    def test_has_drift_true_when_drifted_fields_present(self):
        entry = CatalogEntry(service="svc", results=[_make("svc", ["timeout"])])
        assert entry.has_drift is True

    def test_drift_count_counts_only_drifted_results(self):
        entry = CatalogEntry(
            service="svc",
            results=[_make("svc"), _make("svc", ["x"]), _make("svc", ["y"])],
        )
        assert entry.drift_count == 2

    def test_to_dict_contains_expected_keys(self):
        entry = CatalogEntry(service="svc", results=[_make("svc", ["k"])])
        d = entry.to_dict()
        assert set(d.keys()) == {"service", "total_results", "drift_count", "has_drift"}

    def test_to_dict_values_correct(self):
        entry = CatalogEntry(service="svc", results=[_make("svc", ["k"]), _make("svc")])
        d = entry.to_dict()
        assert d["service"] == "svc"
        assert d["total_results"] == 2
        assert d["drift_count"] == 1
        assert d["has_drift"] is True


# ---------------------------------------------------------------------------
# build_catalog
# ---------------------------------------------------------------------------

class TestBuildCatalog:
    def test_none_raises(self):
        with pytest.raises(CatalogerError):
            build_catalog(None)

    def test_empty_list_returns_empty_catalog(self):
        report = build_catalog([])
        assert report.total_services() == 0

    def test_single_service_grouped(self):
        results = [_make("auth"), _make("auth", ["timeout"])]
        report = build_catalog(results)
        assert report.total_services() == 1
        assert report.get("auth").drift_count == 1

    def test_multiple_services_grouped_separately(self):
        results = [_make("auth"), _make("billing", ["rate"]), _make("auth", ["ttl"])]
        report = build_catalog(results)
        assert report.total_services() == 2
        assert report.get("auth").drift_count == 1
        assert report.get("billing").drift_count == 1

    def test_service_names_sorted(self):
        results = [_make("zebra"), _make("alpha"), _make("mango")]
        report = build_catalog(results)
        assert report.service_names() == ["alpha", "mango", "zebra"]

    def test_drifted_services_only_includes_drifted(self):
        results = [_make("clean"), _make("dirty", ["field"])]
        report = build_catalog(results)
        assert report.drifted_services() == ["dirty"]

    def test_summary_format(self):
        results = [_make("a"), _make("b", ["x"]), _make("c", ["y"])]
        report = build_catalog(results)
        assert report.summary() == "2/3 services have drift"

    def test_get_missing_service_returns_none(self):
        report = build_catalog([_make("svc")])
        assert report.get("nonexistent") is None

    def test_empty_service_name_raises(self):
        with pytest.raises(CatalogerError):
            build_catalog([DriftResult(service="", drifted_fields=[])])
