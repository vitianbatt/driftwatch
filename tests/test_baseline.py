"""Tests for driftwatch.baseline."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from driftwatch.baseline import (
    BaselineEntry,
    BaselineError,
    load_baseline,
    save_baseline,
)


# ---------------------------------------------------------------------------
# BaselineEntry
# ---------------------------------------------------------------------------

class TestBaselineEntry:
    def test_to_dict_round_trip(self):
        entry = BaselineEntry(
            service="api",
            snapshot={"replicas": 3},
            recorded_at="2024-01-01T00:00:00+00:00",
        )
        d = entry.to_dict()
        restored = BaselineEntry.from_dict(d)
        assert restored.service == entry.service
        assert restored.snapshot == entry.snapshot
        assert restored.recorded_at == entry.recorded_at

    def test_from_dict_requires_keys(self):
        with pytest.raises(KeyError):
            BaselineEntry.from_dict({"service": "x"})


# ---------------------------------------------------------------------------
# save_baseline
# ---------------------------------------------------------------------------

def test_save_creates_file(tmp_path):
    p = tmp_path / "baselines.jsonl"
    entry = save_baseline(p, "svc-a", {"env": "prod"})
    assert p.exists()
    assert entry.service == "svc-a"


def test_save_appends_multiple(tmp_path):
    p = tmp_path / "baselines.jsonl"
    save_baseline(p, "svc-a", {"v": 1})
    save_baseline(p, "svc-a", {"v": 2})
    lines = p.read_text().splitlines()
    assert len(lines) == 2


def test_save_uses_provided_timestamp(tmp_path):
    p = tmp_path / "baselines.jsonl"
    entry = save_baseline(p, "svc", {}, recorded_at="2000-01-01T00:00:00+00:00")
    assert entry.recorded_at == "2000-01-01T00:00:00+00:00"


def test_save_auto_timestamp_set(tmp_path):
    p = tmp_path / "baselines.jsonl"
    entry = save_baseline(p, "svc", {})
    assert entry.recorded_at  # non-empty


def test_save_raises_on_bad_path():
    with pytest.raises(BaselineError):
        save_baseline("/no/such/dir/file.jsonl", "svc", {})


# ---------------------------------------------------------------------------
# load_baseline
# ---------------------------------------------------------------------------

def test_load_returns_none_when_file_missing(tmp_path):
    result = load_baseline(tmp_path / "nope.jsonl", "svc")
    assert result is None


def test_load_returns_none_when_service_absent(tmp_path):
    p = tmp_path / "b.jsonl"
    save_baseline(p, "other", {})
    assert load_baseline(p, "svc") is None


def test_load_returns_latest_entry(tmp_path):
    p = tmp_path / "b.jsonl"
    save_baseline(p, "svc", {"v": 1}, recorded_at="2024-01-01T00:00:00+00:00")
    save_baseline(p, "svc", {"v": 2}, recorded_at="2024-06-01T00:00:00+00:00")
    entry = load_baseline(p, "svc")
    assert entry.snapshot["v"] == 2


def test_load_ignores_other_services(tmp_path):
    p = tmp_path / "b.jsonl"
    save_baseline(p, "alpha", {"x": 1})
    save_baseline(p, "beta", {"x": 99})
    entry = load_baseline(p, "alpha")
    assert entry.snapshot["x"] == 1


def test_load_raises_on_malformed_line(tmp_path):
    p = tmp_path / "b.jsonl"
    p.write_text("not-json\n")
    with pytest.raises(BaselineError, match="Malformed"):
        load_baseline(p, "svc")
