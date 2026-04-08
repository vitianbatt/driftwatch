"""Tests for driftwatch.watcher."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from driftwatch.comparator import DriftResult
from driftwatch.watcher import WatchError, WatchTarget, fetch_live_config, watch, watch_all


SAMPLE_SPEC_PATH = "tests/fixtures/sample_spec.yaml"


def _make_target(
    name: str = "svc",
    live_url: str = "http://svc.internal/config",
    spec_path: str = SAMPLE_SPEC_PATH,
) -> WatchTarget:
    return WatchTarget(name=name, spec_path=spec_path, live_url=live_url)


# ---------------------------------------------------------------------------
# fetch_live_config
# ---------------------------------------------------------------------------


class TestFetchLiveConfig:
    def test_returns_json_on_success(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"key": "value"}

        with patch("driftwatch.watcher.requests.get", return_value=mock_resp):
            result = fetch_live_config(_make_target())

        assert result == {"key": "value"}

    def test_raises_on_non_200(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 503

        with patch("driftwatch.watcher.requests.get", return_value=mock_resp):
            with pytest.raises(WatchError, match="HTTP 503"):
                fetch_live_config(_make_target())

    def test_raises_on_network_error(self):
        with patch(
            "driftwatch.watcher.requests.get",
            side_effect=requests.ConnectionError("refused"),
        ):
            with pytest.raises(WatchError, match="Failed to reach"):
                fetch_live_config(_make_target())

    def test_raises_on_invalid_json(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.side_effect = ValueError("not json")

        with patch("driftwatch.watcher.requests.get", return_value=mock_resp):
            with pytest.raises(WatchError, match="non-JSON"):
                fetch_live_config(_make_target())


# ---------------------------------------------------------------------------
# watch
# ---------------------------------------------------------------------------


class TestWatch:
    def test_returns_drift_result(self):
        live_payload = {"version": "1.0", "replicas": 3, "env": "production"}

        with patch(
            "driftwatch.watcher.fetch_live_config", return_value=live_payload
        ):
            result = watch(_make_target())

        assert isinstance(result, DriftResult)
        assert result.service_name == "svc"

    def test_raises_on_bad_spec_path(self):
        target = _make_target(spec_path="nonexistent/path.yaml")
        with patch("driftwatch.watcher.fetch_live_config", return_value={}):
            with pytest.raises(WatchError, match="Could not load spec"):
                watch(target)


# ---------------------------------------------------------------------------
# watch_all
# ---------------------------------------------------------------------------


class TestWatchAll:
    def test_collects_results_for_all_targets(self):
        targets = [_make_target(name="a"), _make_target(name="b")]
        live_payload = {"version": "1.0", "replicas": 3, "env": "production"}

        with patch("driftwatch.watcher.fetch_live_config", return_value=live_payload):
            results = watch_all(targets)

        assert len(results) == 2
        assert {r.service_name for r in results} == {"a", "b"}

    def test_captures_errors_without_raising(self):
        targets = [_make_target(name="broken", live_url="http://bad")]

        with patch(
            "driftwatch.watcher.fetch_live_config",
            side_effect=WatchError("connection refused"),
        ):
            results = watch_all(targets)

        assert len(results) == 1
        assert results[0].error is not None
        assert "connection refused" in results[0].error
