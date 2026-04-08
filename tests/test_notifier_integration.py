"""Integration-style tests: load notifier config from fixture then notify."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.loader import load_notifier_config
from driftwatch.notifier import NotifierConfig, NotifyChannel, NotifyError, notify

FIXTURE = Path(__file__).parent / "fixtures" / "sample_notifier_config.yaml"


def _make_result(service: str, missing=None) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=missing or [],
        extra_keys=[],
        changed_keys={},
    )


class TestLoadNotifierConfigFixture:
    def test_fixture_loads(self):
        data = load_notifier_config(FIXTURE)
        assert data["channel"] == "webhook"
        assert "webhook_url" in data
        assert data["timeout"] == 10

    def test_config_constructed_from_fixture(self):
        data = load_notifier_config(FIXTURE)
        cfg = NotifierConfig(
            channel=NotifyChannel(data["channel"]),
            webhook_url=data.get("webhook_url"),
            timeout=data["timeout"],
        )
        assert cfg.channel == NotifyChannel.WEBHOOK
        assert cfg.timeout == 10


class TestNotifyIntegration:
    def _cfg(self) -> NotifierConfig:
        data = load_notifier_config(FIXTURE)
        return NotifierConfig(
            channel=NotifyChannel(data["channel"]),
            webhook_url=data["webhook_url"],
            timeout=data["timeout"],
        )

    def _mock_response(self, status: int):
        resp = MagicMock()
        resp.status = status
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_webhook_called_with_correct_content_type(self):
        cfg = self._cfg()
        results = [_make_result("api-gateway", missing=["replicas"])]
        captured = {}

        def fake_urlopen(req, timeout):
            captured["headers"] = dict(req.headers)
            captured["method"] = req.method
            return self._mock_response(200)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            notify(results, cfg)

        assert captured["method"] == "POST"
        assert captured["headers"].get("Content-type") == "application/json"

    def test_no_drift_skips_webhook(self):
        cfg = self._cfg()
        with patch("urllib.request.urlopen") as mock_open:
            notify([_make_result("svc-ok")], cfg)
            mock_open.assert_not_called()

    def test_multiple_drifted_services_all_in_payload(self):
        cfg = self._cfg()
        results = [
            _make_result("svc-a", missing=["port"]),
            _make_result("svc-b", missing=["env"]),
        ]
        captured = {}

        def fake_urlopen(req, timeout):
            import json
            captured["body"] = json.loads(req.data)
            return self._mock_response(202)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            notify(results, cfg)

        assert captured["body"]["drifted_count"] == 2
        service_names = [s["service"] for s in captured["body"]["services"]]
        assert "svc-a" in service_names
        assert "svc-b" in service_names
