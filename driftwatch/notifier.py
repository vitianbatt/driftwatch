"""Notification module for driftwatch — sends alerts when drift is detected."""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity

logger = logging.getLogger(__name__)


class NotifyError(Exception):
    """Raised when a notification cannot be delivered."""


class NotifyChannel(str, Enum):
    WEBHOOK = "webhook"
    LOG = "log"


@dataclass
class NotifierConfig:
    channel: NotifyChannel
    webhook_url: Optional[str] = None
    min_severity: Severity = Severity.MEDIUM
    timeout: int = 5

    def __post_init__(self) -> None:
        if self.channel == NotifyChannel.WEBHOOK and not self.webhook_url:
            raise NotifyError("webhook_url is required when channel is 'webhook'")
        if self.timeout <= 0:
            raise NotifyError("timeout must be a positive integer")


def _build_payload(results: List[DriftResult], severity: Severity) -> dict:
    """Build a JSON-serialisable payload summarising drifted results."""
    drifted = [
        {
            "service": r.service,
            "missing": r.missing_keys,
            "extra": r.extra_keys,
            "changed": r.changed_keys,
        }
        for r in results
        if r.has_drift
    ]
    return {
        "alert": "drift_detected",
        "min_severity": severity.value,
        "drifted_count": len(drifted),
        "services": drifted,
    }


def notify(results: List[DriftResult], config: NotifierConfig) -> None:
    """Send drift notifications according to *config*.

    Only results that contain actual drift are included in the alert.
    If no results have drift, the function returns silently.
    """
    drifted = [r for r in results if r.has_drift]
    if not drifted:
        logger.debug("notify: no drift detected — skipping notification")
        return

    if config.channel == NotifyChannel.LOG:
        for r in drifted:
            logger.warning("DRIFT detected in service '%s': %s", r.service, r.summary)
        return

    payload = _build_payload(drifted, config.min_severity)
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        config.webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=config.timeout) as resp:
            status = resp.status
    except Exception as exc:
        raise NotifyError(f"Failed to deliver webhook notification: {exc}") from exc

    if status not in (200, 201, 202, 204):
        raise NotifyError(f"Webhook returned unexpected status {status}")
    logger.info("notify: webhook delivered (HTTP %s)", status)
