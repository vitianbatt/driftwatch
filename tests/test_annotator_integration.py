"""Integration tests: load fixture annotation map and run annotate_results."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.annotator import annotate_results, filter_by_note

FIXTURE = Path(__file__).parent / "fixtures" / "sample_annotation_map.yaml"


def _make(service: str, drift_fields=None) -> DriftResult:
    return DriftResult(
        service=service,
        has_drift=bool(drift_fields),
        drift_fields=drift_fields or [],
    )


@pytest.fixture()
def note_map():
    with FIXTURE.open() as fh:
        return yaml.safe_load(fh)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_services_in_map(self, note_map):
        assert len(note_map) == 4

    def test_auth_has_two_notes(self, note_map):
        assert len(note_map["auth-service"]) == 2

    def test_worker_has_empty_notes(self, note_map):
        assert note_map["worker-service"] == []


class TestAnnotateWithFixture:
    def test_all_services_annotated(self, note_map):
        results = [
            _make("auth-service", ["env"]),
            _make("billing-service"),
            _make("gateway-service", ["replicas"]),
            _make("worker-service"),
        ]
        out = annotate_results(results, note_map)
        assert len(out) == 4

    def test_unknown_service_gets_empty_notes(self, note_map):
        results = [_make("unknown-service")]
        out = annotate_results(results, note_map)
        assert out[0].notes == []

    def test_auth_notes_preserved(self, note_map):
        results = [_make("auth-service")]
        out = annotate_results(results, note_map)
        assert "Reviewed 2024-01-15" in out[0].notes

    def test_filter_by_keyword_rotation(self, note_map):
        results = [
            _make("auth-service"),
            _make("billing-service"),
            _make("gateway-service"),
            _make("worker-service"),
        ]
        annotated = annotate_results(results, note_map)
        out = filter_by_note(annotated, "rotation")
        assert len(out) == 1
        assert out[0].result.service == "auth-service"

    def test_filter_by_keyword_team(self, note_map):
        results = [
            _make("auth-service"),
            _make("billing-service"),
            _make("gateway-service"),
        ]
        annotated = annotate_results(results, note_map)
        out = filter_by_note(annotated, "team")
        assert len(out) == 1
        assert out[0].result.service == "billing-service"
