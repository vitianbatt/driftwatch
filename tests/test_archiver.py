"""Tests for driftwatch.archiver."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from driftwatch.archiver import (
    ArchiveEntry,
    ArchiverError,
    archive_results,
    load_archive,
)
from driftwatch.comparator import DriftResult


def _make(service: str, missing=(), extra=(), changed=()) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=set(missing),
        extra_keys=set(extra),
        changed_keys=set(changed),
    )


# ---------------------------------------------------------------------------
# ArchiveEntry
# ---------------------------------------------------------------------------

class TestArchiveEntry:
    def test_to_dict_round_trip(self):
        entry = ArchiveEntry(
            service="svc", archived_at="2024-01-01T00:00:00",
            drifted=True, drift_fields=["port"]
        )
        d = entry.to_dict()
        restored = ArchiveEntry.from_dict(d)
        assert restored.service == "svc"
        assert restored.drifted is True
        assert restored.drift_fields == ["port"]

    def test_from_dict_missing_key_raises(self):
        with pytest.raises(ArchiverError, match="missing keys"):
            ArchiveEntry.from_dict({"service": "x", "drifted": False})

    def test_drift_fields_defaults_empty(self):
        entry = ArchiveEntry.from_dict(
            {"service": "x", "archived_at": "2024-01-01", "drifted": False}
        )
        assert entry.drift_fields == []


# ---------------------------------------------------------------------------
# archive_results
# ---------------------------------------------------------------------------

class TestArchiveResults:
    def test_creates_file_in_bucket(self, tmp_path):
        results = [_make("api", missing=["timeout"])]
        out = archive_results(results, tmp_path, bucket="2024-06-01")
        assert out == tmp_path / "2024-06-01.jsonl"
        assert out.exists()

    def test_written_lines_are_valid_json(self, tmp_path):
        results = [_make("api"), _make("db", extra=["debug"])]
        out = archive_results(results, tmp_path, bucket="2024-06-01")
        lines = out.read_text().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            json.loads(line)  # must not raise

    def test_appends_on_second_call(self, tmp_path):
        results = [_make("svc")]
        archive_results(results, tmp_path, bucket="2024-06-01")
        archive_results(results, tmp_path, bucket="2024-06-01")
        lines = (tmp_path / "2024-06-01.jsonl").read_text().strip().splitlines()
        assert len(lines) == 2

    def test_none_results_raises(self, tmp_path):
        with pytest.raises(ArchiverError):
            archive_results(None, tmp_path, bucket="2024-06-01")  # type: ignore

    def test_creates_archive_dir_if_missing(self, tmp_path):
        nested = tmp_path / "deep" / "archive"
        archive_results([_make("x")], nested, bucket="2024-06-01")
        assert nested.exists()


# ---------------------------------------------------------------------------
# load_archive
# ---------------------------------------------------------------------------

class TestLoadArchive:
    def test_load_returns_entries(self, tmp_path):
        results = [_make("api", missing=["port"]), _make("db")]
        archive_results(results, tmp_path, bucket="2024-06-01")
        entries = load_archive(tmp_path, "2024-06-01")
        assert len(entries) == 2
        services = {e.service for e in entries}
        assert services == {"api", "db"}

    def test_missing_bucket_raises(self, tmp_path):
        with pytest.raises(ArchiverError, match="not found"):
            load_archive(tmp_path, "1999-01-01")

    def test_drifted_flag_correct(self, tmp_path):
        archive_results([_make("svc", missing=["x"])], tmp_path, bucket="2024-06-01")
        entries = load_archive(tmp_path, "2024-06-01")
        assert entries[0].drifted is True

    def test_clean_service_not_drifted(self, tmp_path):
        archive_results([_make("clean")], tmp_path, bucket="2024-06-02")
        entries = load_archive(tmp_path, "2024-06-02")
        assert entries[0].drifted is False
