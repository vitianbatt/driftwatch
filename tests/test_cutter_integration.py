"""Integration tests for cutter using the sample fixture."""
import pytest
import yaml
from pathlib import Path
from driftwatch.cutter import CutConfig, CutResult, cut_results
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff

FIXTURE = Path(__file__).parent / "fixtures" / "sample_cutter_input.yaml"


def _load_fixture():
    with FIXTURE.open() as fh:
        data = yaml.safe_load(fh)
    results = []
    for entry in data["results"]:
        r = DriftResult(service=entry["service"])
        r.diffs = [
            FieldDiff(field=d["field"], expected=d["expected"], actual=d["actual"])
            for d in entry.get("diffs", [])
        ]
        results.append(r)
    return results


@pytest.fixture
def results():
    return _load_fixture()


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_three_results_loaded(self, results):
        assert len(results) == 3

    def test_auth_service_has_three_diffs(self, results):
        auth = next(r for r in results if r.service == "auth-service")
        assert len(auth.diffs) == 3

    def test_gateway_service_is_clean(self, results):
        gw = next(r for r in results if r.service == "gateway-service")
        assert len(gw.diffs) == 0


class TestCutterIntegration:
    def test_prefix_env_keeps_two_for_auth(self, results):
        report = cut_results(results, CutConfig(prefix="env_"))
        auth = next(r for r in report.results if r.service == "auth-service")
        assert len(auth.diffs) == 2
        assert all(d.field.startswith("env_") for d in auth.diffs)

    def test_prefix_env_keeps_one_for_billing(self, results):
        report = cut_results(results, CutConfig(prefix="env_"))
        billing = next(r for r in report.results if r.service == "billing-service")
        assert len(billing.diffs) == 1
        assert billing.diffs[0].field == "env_region"

    def test_gateway_stays_clean_after_cut(self, results):
        report = cut_results(results, CutConfig(prefix="env_"))
        gw = next(r for r in report.results if r.service == "gateway-service")
        assert gw.has_drift() is False

    def test_total_with_drift_after_prefix_cut(self, results):
        report = cut_results(results, CutConfig(prefix="env_"))
        assert report.total_with_drift() == 2

    def test_suffix_timeout_isolates_one_field(self, results):
        report = cut_results(results, CutConfig(suffix="_timeout"))
        auth = next(r for r in report.results if r.service == "auth-service")
        assert len(auth.diffs) == 1
        assert auth.diffs[0].field == "env_timeout"

    def test_service_names_order_preserved(self, results):
        report = cut_results(results, CutConfig(prefix="env_"))
        assert report.service_names() == ["auth-service", "billing-service", "gateway-service"]
