"""Tests for driftwatch.exporter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from driftwatch.auditor import AuditEntry
from driftwatch.exporter import ExportError, ExportFormat, export_csv, export_entries, export_jsonl


def _make_entry(service: str, has_drift: bool, fields: list[str] | None = None) -> AuditEntry:
    return AuditEntry(
        timestamp="2024-06-01T12:00:00Z",
        service=service,
        has_drift=has_drift,
        drift_fields=fields or [],
        spec_source="spec.yaml",
    )


class TestExportJsonl:
    def test_empty_entries_returns_empty_string(self):
        assert export_jsonl([]) == ""

    def test_single_entry_is_valid_json(self):
        entry = _make_entry("svc-a", False)
        output = export_jsonl([entry])
        parsed = json.loads(output.strip())
        assert parsed["service"] == "svc-a"
        assert parsed["has_drift"] is False

    def test_multiple_entries_produce_multiple_lines(self):
        entries = [_make_entry("svc-a", False), _make_entry("svc-b", True, ["replicas"])]
        lines = export_jsonl(entries).strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[1])["service"] == "svc-b"

    def test_drift_fields_preserved(self):
        entry = _make_entry("svc-c", True, ["image", "env.PORT"])
        data = json.loads(export_jsonl([entry]).strip())
        assert data["drift_fields"] == ["image", "env.PORT"]


class TestExportCsv:
    def test_empty_entries_returns_header_only(self):
        output = export_csv([])
        assert output.strip() == "timestamp,service,has_drift,drift_fields,spec_source"

    def test_single_row_contains_service(self):
        entry = _make_entry("svc-a", False)
        rows = export_csv([entry]).splitlines()
        assert "svc-a" in rows[1]

    def test_drift_fields_joined_by_pipe(self):
        entry = _make_entry("svc-b", True, ["replicas", "image"])
        rows = export_csv([entry]).splitlines()
        assert "replicas|image" in rows[1]

    def test_has_drift_is_lowercase_bool(self):
        entry = _make_entry("svc-c", True, ["x"])
        rows = export_csv([entry]).splitlines()
        assert "true" in rows[1]


class TestExportEntries:
    def test_jsonl_format_dispatches_correctly(self):
        entry = _make_entry("svc-a", False)
        result = export_entries([entry], ExportFormat.JSONL)
        assert json.loads(result.strip())["service"] == "svc-a"

    def test_csv_format_dispatches_correctly(self):
        entry = _make_entry("svc-a", False)
        result = export_entries([entry], ExportFormat.CSV)
        assert "svc-a" in result

    def test_writes_file_when_dest_given(self, tmp_path: Path):
        dest = tmp_path / "out.jsonl"
        entry = _make_entry("svc-x", False)
        export_entries([entry], ExportFormat.JSONL, dest=dest)
        assert dest.exists()
        assert "svc-x" in dest.read_text()

    def test_raises_export_error_on_bad_path(self):
        bad = Path("/no/such/directory/out.csv")
        with pytest.raises(ExportError, match="Failed to write"):
            export_entries([], ExportFormat.CSV, dest=bad)
