"""Tests for driftwatch.streamer."""
from __future__ import annotations

import io
import json

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.streamer import StreamConfig, StreamerError, stream_results


def _make(service: str, missing=None, extra=None, changed=None) -> DriftResult:
    missing = missing or []
    extra = extra or []
    changed = changed or []
    return DriftResult(
        service=service,
        missing_keys=missing,
        extra_keys=extra,
        changed_keys=changed,
    )


# ---------------------------------------------------------------------------
# StreamConfig validation
# ---------------------------------------------------------------------------

class TestStreamConfig:
    def test_default_format_is_jsonl(self):
        cfg = StreamConfig()
        assert cfg.format == "jsonl"

    def test_text_format_accepted(self):
        cfg = StreamConfig(format="text")
        assert cfg.format == "text"

    def test_invalid_format_raises(self):
        with pytest.raises(StreamerError, match="Unsupported stream format"):
            StreamConfig(format="csv")


# ---------------------------------------------------------------------------
# JSONL streaming
# ---------------------------------------------------------------------------

class TestStreamResultsJsonl:
    def test_empty_iterable_writes_nothing(self):
        buf = io.StringIO()
        count = stream_results([], StreamConfig(out=buf))
        assert count == 0
        assert buf.getvalue() == ""

    def test_clean_result_written(self):
        buf = io.StringIO()
        stream_results([_make("svc-a")], StreamConfig(out=buf))
        line = buf.getvalue().strip()
        data = json.loads(line)
        assert data["service"] == "svc-a"
        assert data["has_drift"] is False

    def test_drift_result_written(self):
        buf = io.StringIO()
        stream_results(
            [_make("svc-b", missing=["timeout"])],
            StreamConfig(out=buf),
        )
        data = json.loads(buf.getvalue().strip())
        assert data["has_drift"] is True
        assert "timeout" in data["missing_keys"]

    def test_multiple_results_produce_multiple_lines(self):
        buf = io.StringIO()
        count = stream_results(
            [_make("a"), _make("b"), _make("c")],
            StreamConfig(out=buf),
        )
        assert count == 3
        lines = buf.getvalue().strip().splitlines()
        assert len(lines) == 3

    def test_returns_count(self):
        buf = io.StringIO()
        n = stream_results([_make("x"), _make("y")], StreamConfig(out=buf))
        assert n == 2


# ---------------------------------------------------------------------------
# Text streaming
# ---------------------------------------------------------------------------

class TestStreamResultsText:
    def _cfg(self) -> tuple[StreamConfig, io.StringIO]:
        buf = io.StringIO()
        return StreamConfig(format="text", out=buf), buf

    def test_clean_result_shows_ok(self):
        cfg, buf = self._cfg()
        stream_results([_make("auth")], cfg)
        assert "[OK]" in buf.getvalue()
        assert "auth" in buf.getvalue()

    def test_drift_result_shows_drift(self):
        cfg, buf = self._cfg()
        stream_results([_make("billing", missing=["retries"])], cfg)
        assert "[DRIFT]" in buf.getvalue()
        assert "missing" in buf.getvalue()

    def test_default_config_uses_stdout(self):
        # Just ensure StreamConfig() doesn't blow up (stdout is default).
        cfg = StreamConfig()
        assert cfg.out is not None
