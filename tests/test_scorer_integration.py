"""Integration tests for scorer using fixture data."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import score_results, ScoredReport

FIXTURE = Path("tests/fixtures/sample_scorer_input.yaml")


def _load_fixture() -> list[DriftResult]:
    data = yaml.safe_load(FIXTURE.read_text())
    results = []
    for item in data:
        diffs = [
            FieldDiff(field=d["field"], expected=str(d["expected"]), actual=str(d["actual"]))
            for d in item.get("diffs") or []
        ]
        results.append(DriftResult(service=item["service"], diffs=diffs))
    return results


@pytest.fixture(scope="module")
def report() -> ScoredReport:
    return score_results(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self):
        results = _load_fixture()
        assert len(results) == 4


class TestScorerIntegration:
    def test_auth_service_score_is_zero(self, report):
        auth = next(r for r in report.results if r.service == "auth-service")
        assert auth.score == 0

    def test_gateway_outranks_payment(self, report):
        gateway = next(r for r in report.results if r.service == "gateway")
        payment = next(r for r in report.results if r.service == "payment-service")
        assert gateway.score >= payment.score

    def test_worker_has_highest_score(self, report):
        worker = next(r for r in report.results if r.service == "worker")
        others = [r for r in report.results if r.service != "worker"]
        assert all(worker.score >= o.score for o in others)

    def test_average_is_positive(self, report):
        assert report.average > 0

    def test_highest_matches_worker(self, report):
        assert report.highest.service == "worker"
