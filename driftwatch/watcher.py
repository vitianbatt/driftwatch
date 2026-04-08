"""Service watcher: fetches live config from running services and compares against specs."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import requests

from driftwatch.comparator import DriftResult, compare
from driftwatch.loader import SpecLoadError, load_spec


class WatchError(Exception):
    """Raised when a watch operation fails."""


@dataclass
class WatchTarget:
    """Describes a single service to watch."""

    name: str
    spec_path: str
    live_url: str
    timeout: float = 5.0
    headers: dict[str, str] = field(default_factory=dict)


def fetch_live_config(target: WatchTarget) -> dict[str, Any]:
    """Fetch live configuration JSON from a running service endpoint.

    Args:
        target: The WatchTarget describing where to fetch from.

    Returns:
        Parsed JSON payload as a dict.

    Raises:
        WatchError: On network failure or non-200 response.
    """
    try:
        response = requests.get(
            target.live_url,
            headers=target.headers,
            timeout=target.timeout,
        )
    except requests.RequestException as exc:
        raise WatchError(
            f"Failed to reach '{target.name}' at {target.live_url}: {exc}"
        ) from exc

    if response.status_code != 200:
        raise WatchError(
            f"Service '{target.name}' returned HTTP {response.status_code} "
            f"from {target.live_url}"
        )

    try:
        return response.json()
    except ValueError as exc:
        raise WatchError(
            f"Service '{target.name}' returned non-JSON body: {exc}"
        ) from exc


def watch(target: WatchTarget) -> DriftResult:
    """Load the spec for *target*, fetch live config, and return a DriftResult.

    Args:
        target: The WatchTarget to evaluate.

    Returns:
        A DriftResult capturing any detected drift.

    Raises:
        WatchError: If spec loading or live fetch fails.
    """
    try:
        spec = load_spec(target.spec_path)
    except SpecLoadError as exc:
        raise WatchError(f"Could not load spec for '{target.name}': {exc}") from exc

    live = fetch_live_config(target)
    return compare(service_name=target.name, spec=spec, live=live)


def watch_all(targets: list[WatchTarget]) -> list[DriftResult]:
    """Run :func:`watch` over every target, collecting results.

    Failures for individual targets are captured as drift entries rather than
    propagating exceptions, so the caller always receives a full result list.
    """
    results: list[DriftResult] = []
    for target in targets:
        try:
            results.append(watch(target))
        except WatchError as exc:
            results.append(
                DriftResult(
                    service_name=target.name,
                    missing_keys=[],
                    mismatched_keys={},
                    extra_keys=[],
                    error=str(exc),
                )
            )
    return results
