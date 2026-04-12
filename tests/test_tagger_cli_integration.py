"""Integration tests for tagger_cli using fixture files."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from driftwatch.tagger_cli import results_from_json, run_tagger
from driftwatch.tagging import tag_results

FIXTURE_INPUT = Path("tests/fixtures/sample_tagger_input.json")
FIXTURE_TAG_MAP = Path("tests/fixtures/sample_tag_map.yaml")


@pytest.fixture()
def raw_results() -> str:
    return FIXTURE_INPUT.read_text()


@pytest.fixture()
def results(raw_results):
    return results_from_json(raw_results)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE_INPUT.exists()

    def test_three_results_loaded(self, results):
        assert len(results) == 3

    def test_auth_service_is_clean(self, results):
        auth = next(r for r in results if r.service == "auth-service")
        assert auth.diffs == []

    def test_payment_service_has_two_diffs(self, results):
        payment = next(r for r in results if r.service == "payment-service")
        assert len(payment.diffs) == 2

    def test_notification_service_has_one_diff(self, results):
        notif = next(r for r in results if r.service == "notification-service")
        assert len(notif.diffs) == 1


class TestRunTaggerWithFixtures:
    def test_tag_map_fixture_exists(self):
        assert FIXTURE_TAG_MAP.exists()

    def test_run_returns_valid_json(self, raw_results):
        out = run_tagger(raw_results, str(FIXTURE_TAG_MAP))
        parsed = json.loads(out)
        assert isinstance(parsed, list)

    def test_all_services_present_without_filter(self, raw_results):
        out = json.loads(run_tagger(raw_results, str(FIXTURE_TAG_MAP)))
        services = {r["service"] for r in out}
        assert "auth-service" in services
        assert "payment-service" in services
        assert "notification-service" in services

    def test_each_result_has_tags_key(self, raw_results):
        out = json.loads(run_tagger(raw_results, str(FIXTURE_TAG_MAP)))
        for entry in out:
            assert "tags" in entry

    def test_each_result_has_drift_fields_key(self, raw_results):
        out = json.loads(run_tagger(raw_results, str(FIXTURE_TAG_MAP)))
        for entry in out:
            assert "drift_fields" in entry
