"""Simple file-based cache for live config snapshots."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


class CacheError(Exception):
    """Raised when cache read/write operations fail."""


@dataclass
class CacheEntry:
    service: str
    config: dict[str, Any]
    fetched_at: float

    def is_stale(self, ttl_seconds: int) -> bool:
        """Return True if the entry is older than ttl_seconds."""
        return (time.time() - self.fetched_at) > ttl_seconds


class ConfigCache:
    """Persist and retrieve live config snapshots on disk."""

    def __init__(self, cache_dir: str | Path) -> None:
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, service: str) -> Path:
        safe = service.replace("/", "_").replace(":", "_")
        return self._dir / f"{safe}.json"

    def store(self, service: str, config: dict[str, Any]) -> None:
        """Write a config snapshot to disk."""
        entry = {"service": service, "config": config, "fetched_at": time.time()}
        try:
            self._path(service).write_text(json.dumps(entry, indent=2))
        except OSError as exc:
            raise CacheError(f"Failed to write cache for '{service}': {exc}") from exc

    def load(self, service: str) -> Optional[CacheEntry]:
        """Return a CacheEntry for *service*, or None if not cached."""
        path = self._path(service)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise CacheError(f"Failed to read cache for '{service}': {exc}") from exc
        return CacheEntry(
            service=data["service"],
            config=data["config"],
            fetched_at=data["fetched_at"],
        )

    def invalidate(self, service: str) -> None:
        """Remove the cached entry for *service* if it exists."""
        path = self._path(service)
        if path.exists():
            try:
                path.unlink()
            except OSError as exc:
                raise CacheError(f"Failed to invalidate cache for '{service}': {exc}") from exc

    def clear(self) -> None:
        """Remove all cached entries."""
        for p in self._dir.glob("*.json"):
            try:
                p.unlink()
            except OSError as exc:
                raise CacheError(f"Failed to clear cache entry '{p}': {exc}") from exc
