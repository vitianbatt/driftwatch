"""Integration tests for ranker using the YAML fixture."""
import pytest
import yaml
from pathlib import Path

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.filter import Severity
from driftwatch.ranker import rank_results, RankedReport

FIXTURE = Path(__file__).parent / "fixtures" / "sample_ranker_input.yaml"


def _load_fixture() -> list[DriftResult]:
    data = yaml.safe_load(FIXTURE.read_text())
    results = []
    for entry in data:
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


@pytest.fixture(scope="module")
def report() -> RankedReport:
    return rank_results(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self):
        results = _load_fixture()
        assert len(results) == 4


class TestRankerIntegration:
    def test_auth_service_ranks_first(self, report):
        assert report.ranked[0].result.service == "auth-service"

    def test_notification_service_ranks_last(self, report):
        assert report.ranked[-1].result.service == "notification-service"

    def test_all_ranks_are_unique(self, report):
        ranks = [rr.rank for rr in report.ranked]
        assert len(ranks) == len(set(ranks))

    def test_auth_service_severity_is_high(self, report):
        auth = next(rr for rr in report.ranked if rr.result.service == "auth-service")
        assert auth.severity == Severity.HIGH

    def test_notification_service_severity_is_low(self, report):
        notif = next(rr for rr in report.ranked if rr.result.service == "notification-service")
        assert notif.severity == Severity.LOW

    def test_top_two_excludes_clean_service(self, report):
        top2 = report.top(2)
        services = [rr.result.service for rr in top2]
        assert "notification-service" not in services

    def test_summary_is_non_empty_string(self, report):
        s = report.summary()
        assert isinstance(s, str) and len(s) > 0

    def test_to_dict_for_all_results(self, report):
        for rr in report.ranked:
            d = rr.to_dict()
            assert "service" in d
            assert "rank" in d
            assert d["rank"] >= 1
