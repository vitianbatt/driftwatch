"""Integration tests for auditor using fixture file."""

from __future__ import annotations

from pathlib import Path

import pytest

from driftwatch.auditor import AuditEntry, read_audit_log, append_audit_log, build_entry
from driftwatch.comparator import DriftResult

FIXTURE_LOG = Path(__file__).parent / "fixtures" / "sample_audit_log.jsonl"


class TestReadFixtureLog:
    def test_fixture_loads_three_entries(self):
        entries = read_audit_log(FIXTURE_LOG)
        assert len(entries) == 3

    def test_first_entry_no_drift(self):
        entries = read_audit_log(FIXTURE_LOG)
        first = entries[0]
        assert first.service == "api-gateway"
        assert first.has_drift is False
        assert first.drift_count == 0
        assert first.fields == []

    def test_second_entry_has_drift(self):
        entries = read_audit_log(FIXTURE_LOG)
        second = entries[1]
        assert second.service == "auth-service"
        assert second.has_drift is True
        assert second.drift_count == 2
        assert "replicas" in second.fields
        assert "image_tag" in second.fields

    def test_all_entries_have_timestamps(self):
        entries = read_audit_log(FIXTURE_LOG)
        for e in entries:
            assert e.timestamp

    def test_services_are_unique(self):
        entries = read_audit_log(FIXTURE_LOG)
        services = [e.service for e in entries]
        assert len(services) == len(set(services))


class TestRoundTrip:
    def test_write_then_read_matches_original(self, tmp_path):
        log = tmp_path / "rt.log"
        results = [
            DriftResult(service="svc-1", diffs={}, has_drift=False),
            DriftResult(service="svc-2", diffs={"k": "a != b"}, has_drift=True),
        ]
        for r in results:
            append_audit_log(build_entry(r, timestamp="2024-01-01T00:00:00+00:00"), log)

        entries = read_audit_log(log)
        assert len(entries) == 2
        assert entries[0].service == "svc-1"
        assert entries[0].has_drift is False
        assert entries[1].service == "svc-2"
        assert entries[1].drift_count == 1
        assert entries[1].fields == ["k"]

    def test_entry_to_dict_roundtrip(self):
        original = AuditEntry(
            service="svc",
            timestamp="2024-01-01T00:00:00+00:00",
            has_drift=True,
            drift_count=1,
            fields=["timeout"],
        )
        d = original.to_dict()
        restored = AuditEntry(
            service=d["service"],
            timestamp=d["timestamp"],
            has_drift=d["has_drift"],
            drift_count=d["drift_count"],
            fields=d["fields"],
        )
        assert restored.service == original.service
        assert restored.fields == original.fields
        assert restored.drift_count == original.drift_count
