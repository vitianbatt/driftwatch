"""Tests for driftwatch.notifier."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity
from driftwatch.notifier import (
    NotifierConfig,
    NotifyChannel,
    NotifyError,
    _build_payload,
    notify,
)


def _make_result(service: str, missing=None, extra=None, changed=None) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=missing or [],
        extra_keys=extra or [],
        changed_keys=changed or {},
    )


# ---------------------------------------------------------------------------
# NotifierConfig validation
# ---------------------------------------------------------------------------

class TestNotifierConfig:
    def test_log_channel_requires_no_url(self):
        cfg = NotifierConfig(channel=NotifyChannel.LOG)
        assert cfg.channel == NotifyChannel.LOG

    def test_webhook_requires_url(self):
        with pytest.raises(NotifyError, match="webhook_url is required"):
            NotifierConfig(channel=NotifyChannel.WEBHOOK)

    def test_webhook_with_url_ok(self):
        cfg = NotifierConfig(channel=NotifyChannel.WEBHOOK, webhook_url="http://x.test/hook")
        assert cfg.webhook_url == "http://x.test/hook"

    def test_zero_timeout_raises(self):
        with pytest.raises(NotifyError, match="timeout must be a positive integer"):
            NotifierConfig(channel=NotifyChannel.LOG, timeout=0)

    def test_negative_timeout_raises(self):
        with pytest.raises(NotifyError, match="timeout must be a positive integer"):
            NotifierConfig(channel=NotifyChannel.LOG, timeout=-3)


# ---------------------------------------------------------------------------
# _build_payload
# ---------------------------------------------------------------------------

def test_build_payload_includes_only_drifted():
    results = [
        _make_result("svc-a", missing=["port"]),
        _make_result("svc-b"),
    ]
    payload = _build_payload(results, Severity.MEDIUM)
    assert payload["drifted_count"] == 1
    assert payload["services"][0]["service"] == "svc-a"


def test_build_payload_keys():
    results = [_make_result("svc-a", missing=["port"], changed={"replicas": (2, 3)})]
    payload = _build_payload(results, Severity.HIGH)
    svc = payload["services"][0]
    assert "missing" in svc
    assert "changed" in svc
    assert payload["min_severity"] == Severity.HIGH.value


# ---------------------------------------------------------------------------
# notify — log channel
# ---------------------------------------------------------------------------

def test_notify_log_no_drift_is_silent(caplog):
    cfg = NotifierConfig(channel=NotifyChannel.LOG)
    notify([_make_result("svc-clean")], cfg)
    assert "DRIFT" not in caplog.text


def test_notify_log_emits_warning_on_drift(caplog):
    import logging
    cfg = NotifierConfig(channel=NotifyChannel.LOG)
    with caplog.at_level(logging.WARNING, logger="driftwatch.notifier"):
        notify([_make_result("svc-a", missing=["env"])], cfg)
    assert "svc-a" in caplog.text


# ---------------------------------------------------------------------------
# notify — webhook channel
# ---------------------------------------------------------------------------

def _mock_response(status: int):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_notify_webhook_success():
    cfg = NotifierConfig(channel=NotifyChannel.WEBHOOK, webhook_url="http://hook.test/")
    results = [_make_result("svc-a", missing=["key"])]
    with patch("urllib.request.urlopen", return_value=_mock_response(200)):
        notify(results, cfg)  # should not raise


def test_notify_webhook_bad_status_raises():
    cfg = NotifierConfig(channel=NotifyChannel.WEBHOOK, webhook_url="http://hook.test/")
    results = [_make_result("svc-a", missing=["key"])]
    with patch("urllib.request.urlopen", return_value=_mock_response(500)):
        with pytest.raises(NotifyError, match="500"):
            notify(results, cfg)


def test_notify_webhook_network_error_raises():
    cfg = NotifierConfig(channel=NotifyChannel.WEBHOOK, webhook_url="http://hook.test/")
    results = [_make_result("svc-a", missing=["key"])]
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        with pytest.raises(NotifyError, match="connection refused"):
            notify(results, cfg)


def test_notify_webhook_skipped_when_no_drift():
    cfg = NotifierConfig(channel=NotifyChannel.WEBHOOK, webhook_url="http://hook.test/")
    with patch("urllib.request.urlopen") as mock_open:
        notify([_make_result("svc-clean")], cfg)
        mock_open.assert_not_called()
