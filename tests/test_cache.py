"""Tests for driftwatch.cache."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from driftwatch.cache import CacheEntry, CacheError, ConfigCache


# ---------------------------------------------------------------------------
# CacheEntry
# ---------------------------------------------------------------------------

class TestCacheEntry:
    def test_fresh_entry_not_stale(self):
        entry = CacheEntry(service="svc", config={}, fetched_at=time.time())
        assert not entry.is_stale(ttl_seconds=60)

    def test_old_entry_is_stale(self):
        old_ts = time.time() - 120
        entry = CacheEntry(service="svc", config={}, fetched_at=old_ts)
        assert entry.is_stale(ttl_seconds=60)

    def test_exactly_at_boundary_is_stale(self):
        ts = time.time() - 60
        entry = CacheEntry(service="svc", config={}, fetched_at=ts)
        assert entry.is_stale(ttl_seconds=60)


# ---------------------------------------------------------------------------
# ConfigCache
# ---------------------------------------------------------------------------

@pytest.fixture()
def cache(tmp_path: Path) -> ConfigCache:
    return ConfigCache(cache_dir=tmp_path / "cache")


def test_store_and_load_roundtrip(cache: ConfigCache):
    cfg = {"replicas": 3, "image": "nginx:latest"}
    cache.store("my-service", cfg)
    entry = cache.load("my-service")
    assert entry is not None
    assert entry.service == "my-service"
    assert entry.config == cfg


def test_load_returns_none_for_unknown_service(cache: ConfigCache):
    assert cache.load("nonexistent") is None


def test_store_creates_cache_dir_if_missing(tmp_path: Path):
    deep = tmp_path / "a" / "b" / "c"
    c = ConfigCache(cache_dir=deep)
    c.store("svc", {"k": "v"})
    assert deep.exists()


def test_invalidate_removes_entry(cache: ConfigCache):
    cache.store("svc", {"x": 1})
    cache.invalidate("svc")
    assert cache.load("svc") is None


def test_invalidate_noop_for_missing_service(cache: ConfigCache):
    # Should not raise
    cache.invalidate("ghost")


def test_clear_removes_all_entries(cache: ConfigCache):
    cache.store("svc-a", {"a": 1})
    cache.store("svc-b", {"b": 2})
    cache.clear()
    assert cache.load("svc-a") is None
    assert cache.load("svc-b") is None


def test_service_name_with_slashes_stored_safely(cache: ConfigCache):
    cache.store("namespace/my-service", {"port": 8080})
    entry = cache.load("namespace/my-service")
    assert entry is not None
    assert entry.config["port"] == 8080


def test_corrupt_cache_file_raises(cache: ConfigCache, tmp_path: Path):
    bad = tmp_path / "cache" / "svc.json"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("not valid json")
    with pytest.raises(CacheError, match="Failed to read cache"):
        cache.load("svc")
