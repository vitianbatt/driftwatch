"""Integration tests for baseline using the sample fixture."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from driftwatch.baseline import BaselineEntry, load_baseline

FIXTURE = Path(__file__).parent / "fixtures" / "sample_baseline.jsonl"


class TestReadFixtureBaseline:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_fixture_has_three_lines(self):
        lines = [l for l in FIXTURE.read_text().splitlines() if l.strip()]
        assert len(lines) == 3

    def test_all_lines_are_valid_json(self):
        for line in FIXTURE.read_text().splitlines():
            if line.strip():
                data = json.loads(line)
                assert "service" in data
                assert "snapshot" in data
                assert "recorded_at" in data

    def test_load_auth_service_returns_latest(self):
        entry = load_baseline(FIXTURE, "auth-service")
        assert entry is not None
        assert entry.snapshot["image"] == "auth:1.1.0"
        assert entry.snapshot["replicas"] == 3

    def test_load_gateway_returns_entry(self):
        entry = load_baseline(FIXTURE, "gateway")
        assert entry is not None
        assert entry.snapshot["replicas"] == 3

    def test_load_unknown_service_returns_none(self):
        entry = load_baseline(FIXTURE, "nonexistent")
        assert entry is None

    def test_auth_service_latest_timestamp(self):
        entry = load_baseline(FIXTURE, "auth-service")
        assert entry.recorded_at == "2024-04-15T08:30:00+00:00"

    def test_entry_from_dict_matches_fixture(self):
        line = FIXTURE.read_text().splitlines()[0]
        data = json.loads(line)
        entry = BaselineEntry.from_dict(data)
        assert entry.service == "auth-service"
        assert entry.snapshot["env"] == "production"
