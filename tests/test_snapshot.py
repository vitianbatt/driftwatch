"""Tests for driftwatch.snapshot."""

import json
from pathlib import Path

import pytest

from driftwatch.snapshot import (
    Snapshot,
    SnapshotError,
    load_snapshots,
    save_snapshot,
    take_snapshot,
)

FIXTURE = Path("tests/fixtures/sample_snapshot.jsonl")


# ---------------------------------------------------------------------------
# Snapshot.to_dict / from_dict
# ---------------------------------------------------------------------------

class TestSnapshotRoundTrip:
    def test_to_dict_contains_all_keys(self):
        s = Snapshot(service="svc", timestamp="2024-01-01T00:00:00+00:00", config={"k": 1}, tags=["a"])
        d = s.to_dict()
        assert d["service"] == "svc"
        assert d["config"] == {"k": 1}
        assert d["tags"] == ["a"]

    def test_from_dict_round_trip(self):
        original = Snapshot(service="x", timestamp="t", config={"a": 1}, tags=["tag"])
        restored = Snapshot.from_dict(original.to_dict())
        assert restored == original

    def test_from_dict_missing_key_raises(self):
        with pytest.raises(SnapshotError, match="missing required keys"):
            Snapshot.from_dict({"service": "x", "config": {}})

    def test_tags_default_empty(self):
        s = Snapshot.from_dict({"service": "s", "timestamp": "t", "config": {}})
        assert s.tags == []


# ---------------------------------------------------------------------------
# take_snapshot
# ---------------------------------------------------------------------------

def test_take_snapshot_sets_service_and_config():
    s = take_snapshot("api", {"replicas": 2})
    assert s.service == "api"
    assert s.config == {"replicas": 2}


def test_take_snapshot_uses_provided_timestamp():
    s = take_snapshot("api", {}, timestamp="2024-06-01T00:00:00+00:00")
    assert s.timestamp == "2024-06-01T00:00:00+00:00"


def test_take_snapshot_auto_timestamp():
    s = take_snapshot("api", {})
    assert s.timestamp  # non-empty


# ---------------------------------------------------------------------------
# save_snapshot / load_snapshots
# ---------------------------------------------------------------------------

def test_save_and_load_round_trip(tmp_path):
    p = tmp_path / "snaps.jsonl"
    s = take_snapshot("svc", {"k": "v"}, timestamp="2024-01-01T00:00:00+00:00")
    save_snapshot(s, p)
    loaded = load_snapshots(p)
    assert len(loaded) == 1
    assert loaded[0] == s


def test_save_appends(tmp_path):
    p = tmp_path / "snaps.jsonl"
    s1 = take_snapshot("a", {}, timestamp="t1")
    s2 = take_snapshot("b", {}, timestamp="t2")
    save_snapshot(s1, p)
    save_snapshot(s2, p)
    assert len(load_snapshots(p)) == 2


def test_load_filters_by_service(tmp_path):
    p = tmp_path / "snaps.jsonl"
    save_snapshot(take_snapshot("auth", {}, timestamp="t1"), p)
    save_snapshot(take_snapshot("gateway", {}, timestamp="t2"), p)
    result = load_snapshots(p, service="auth")
    assert all(s.service == "auth" for s in result)
    assert len(result) == 1


def test_load_missing_file_raises(tmp_path):
    with pytest.raises(SnapshotError, match="not found"):
        load_snapshots(tmp_path / "missing.jsonl")


# ---------------------------------------------------------------------------
# Fixture integration
# ---------------------------------------------------------------------------

def test_fixture_has_three_entries():
    snaps = load_snapshots(FIXTURE)
    assert len(snaps) == 3


def test_fixture_auth_snapshots():
    snaps = load_snapshots(FIXTURE, service="auth")
    assert len(snaps) == 2
    assert snaps[0].config["image"] == "auth:1.0"
    assert snaps[1].tags == ["prod", "updated"]
