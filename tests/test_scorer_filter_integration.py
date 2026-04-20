"""Integration tests for scorer_filter using the fixture file."""
import pytest
import yaml
from pathlib import Path

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_filter import ScoreFilterConfig, filter_scored

FIXTURE = Path(__file__).parent / "fixtures" / "sample_scorer_filter_input.yaml"


def _load_fixture() -> ScoredReport:
    data = yaml.safe_load(FIXTURE.read_text())
    results = []
    for entry in data["results"]:
        diffs = [
            FieldDiff(
                field=d["field"],
                kind=d["kind"],
                expected=d.get("expected"),
                actual=d.get("actual"),
            )
            for d in (entry.get("diffs") or [])
        ]
        drift = DriftResult(service=entry["service"], diffs=diffs)
        results.append(ScoredResult(service=entry["service"], score=entry["score"], drift=drift))
    return ScoredReport(results=results)


@pytest.fixture(scope="module")
def report() -> ScoredReport:
    return _load_fixture()


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self, report):
        assert len(report.results) == 4

    def test_service_names_present(self, report):
        names = {r.service for r in report.results}
        assert "auth-service" in names
        assert "payment-service" in names

    def test_auth_service_score_is_zero(self, report):
        auth = next(r for r in report.results if r.service == "auth-service")
        assert auth.score == 0.0


class TestFilterIntegration:
    def test_min_score_three_keeps_two(self, report):
        result = filter_scored(report, ScoreFilterConfig(min_score=3.0))
        assert result.total_kept == 2
        assert result.total_excluded == 2

    def test_exclude_clean_removes_auth(self, report):
        result = filter_scored(report, ScoreFilterConfig(include_clean=False))
        services = {r.service for r in result.results}
        assert "auth-service" not in services
        assert result.total_kept == 3

    def test_max_score_five_excludes_notification(self, report):
        result = filter_scored(report, ScoreFilterConfig(max_score=5.0))
        services = {r.service for r in result.results}
        assert "notification-service" not in services

    def test_combined_filter(self, report):
        cfg = ScoreFilterConfig(min_score=2.0, max_score=5.0, include_clean=False)
        result = filter_scored(report, cfg)
        services = {r.service for r in result.results}
        assert services == {"payment-service", "gateway-service"}
