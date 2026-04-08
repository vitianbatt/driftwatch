"""Tests for driftwatch.auditor."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from driftwatch.auditor import (
    AuditEntry,
    AuditError,
    append_audit_log,
    build_entry,
    read_audit_log,
)
from driftwatch.comparator import DriftResult


def _make_result(service: str, diffs: dict | None = None) -> DriftResult:
    d = diffs or {}
    return DriftResult(service=service, diffs=d, has_drift=bool(d))


# ---------------------------------------------------------------------------
# build_entry
# ---------------------------------------------------------------------------

class TestBuildEntry:
    def test_no_drift_entry(self):
        result = _make_result("svc-a")
        entry = build_entry(result, timestamp="2024-01-01T00:00:00+00:00")
        assert entry.service == "svc-a"
        assert entry.has_drift is False
        assert entry.drift_count == 0
        assert entry.fields == []

    def test_drift_entry_records_fields(self):
        result = _make_result("svc-b", {"replicas": "3 != 2", "image": "a != b"})
        entry = build_entry(result, timestamp="2024-01-01T00:00:00+00:00")
        assert entry.has_drift is True
        assert entry.drift_count == 2
        assert set(entry.fields) == {"replicas", "image"}

    def test_timestamp_auto_set_when_none(self):
        result = _make_result("svc-c")
        entry = build_entry(result)
        assert entry.timestamp  # non-empty
        assert "T" in entry.timestamp  # ISO format

    def test_to_dict_keys(self):
        result = _make_result("svc-d", {"port": "80 != 8080"})
        d = build_entry(result, timestamp="ts").to_dict()
        assert set(d.keys()) == {"service", "timestamp", "has_drift", "drift_count", "fields"}


# ---------------------------------------------------------------------------
# append_audit_log / read_audit_log
# ---------------------------------------------------------------------------

class TestAuditLog:
    def test_write_and_read_single_entry(self, tmp_path):
        log = tmp_path / "audit.log"
        result = _make_result("svc-x", {"env": "prod != staging"})
        entry = build_entry(result, timestamp="2024-06-01T12:00:00+00:00")
        append_audit_log(entry, log)
        entries = read_audit_log(log)
        assert len(entries) == 1
        assert entries[0].service == "svc-x"
        assert entries[0].drift_count == 1

    def test_multiple_entries_appended(self, tmp_path):
        log = tmp_path / "audit.log"
        for i in range(3):
            entry = build_entry(_make_result(f"svc-{i}"), timestamp="ts")
            append_audit_log(entry, log)
        entries = read_audit_log(log)
        assert len(entries) == 3

    def test_read_missing_file_returns_empty(self, tmp_path):
        entries = read_audit_log(tmp_path / "nonexistent.log")
        assert entries == []

    def test_creates_parent_dirs(self, tmp_path):
        log = tmp_path / "nested" / "deep" / "audit.log"
        entry = build_entry(_make_result("svc-y"), timestamp="ts")
        append_audit_log(entry, log)
        assert log.exists()

    def test_corrupt_line_raises_audit_error(self, tmp_path):
        log = tmp_path / "bad.log"
        log.write_text("not-json\n", encoding="utf-8")
        with pytest.raises(AuditError):
            read_audit_log(log)

    def test_write_to_readonly_dir_raises_audit_error(self, tmp_path):
        readonly = tmp_path / "ro"
        readonly.mkdir()
        readonly.chmod(0o444)
        log = readonly / "sub" / "audit.log"
        entry = build_entry(_make_result("svc-z"), timestamp="ts")
        with pytest.raises(AuditError):
            append_audit_log(entry, log)
