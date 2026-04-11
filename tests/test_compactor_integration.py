"""Integration tests for compactor using fixture data."""
import pathlib
import yaml
import pytest

from driftwatch.compactor import compact_results, CompactedResult
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_compactor_input.yaml"


def _load_fixture():
    raw = yaml.safe_load(FIXTURE.read_text())
    results = []
    for item in raw:
        diffs = [
            FieldDiff(field=f, kind="missing", expected=None, actual=None)
            for f in item.get("drift_fields", [])
        ]
        results.append(DriftResult(service=item["service"], drift_fields=diffs))
    return results


@pytest.fixture
def compacted():
    return compact_results(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_five_raw_entries_loaded(self):
        assert len(_load_fixture()) == 5


class TestCompactorIntegration:
    def test_three_unique_services(self, compacted):
        assert len(compacted) == 3

    def test_auth_service_merged(self, compacted):
        auth = next(r for r in compacted if r.service == "auth-service")
        assert auth.source_count == 2

    def test_auth_service_has_three_drift_fields(self, compacted):
        auth = next(r for r in compacted if r.service == "auth-service")
        assert len(auth.drift_fields) == 3

    def test_payments_service_merged(self, compacted):
        pay = next(r for r in compacted if r.service == "payments-service")
        assert pay.source_count == 2

    def test_payments_has_one_drift_field(self, compacted):
        pay = next(r for r in compacted if r.service == "payments-service")
        assert len(pay.drift_fields) == 1

    def test_gateway_is_clean(self, compacted):
        gw = next(r for r in compacted if r.service == "gateway")
        assert not gw.has_drift
        assert gw.source_count == 1

    def test_all_are_compacted_result_instances(self, compacted):
        assert all(isinstance(r, CompactedResult) for r in compacted)
