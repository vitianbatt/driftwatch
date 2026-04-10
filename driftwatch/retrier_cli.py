"""CLI helpers for applying retry policies defined in YAML files."""

from __future__ import annotations

import pathlib
from typing import Any

import yaml

from driftwatch.retrier import RetryPolicy, RetrierError


def load_retry_policy(path: str | pathlib.Path) -> RetryPolicy:
    """Load a RetryPolicy from a YAML file.

    Expected keys: max_attempts, backoff_seconds, backoff_multiplier.
    All keys are optional; missing keys fall back to RetryPolicy defaults.
    """
    resolved = pathlib.Path(path)
    if not resolved.exists():
        raise RetrierError(f"Retry policy file not found: {path}")

    raw: dict[str, Any] = yaml.safe_load(resolved.read_text()) or {}

    try:
        return RetryPolicy(
            max_attempts=int(raw.get("max_attempts", 3)),
            backoff_seconds=float(raw.get("backoff_seconds", 1.0)),
            backoff_multiplier=float(raw.get("backoff_multiplier", 2.0)),
        )
    except RetrierError:
        raise
    except (TypeError, ValueError) as exc:
        raise RetrierError(f"Invalid retry policy in {path}: {exc}") from exc


def policy_to_dict(policy: RetryPolicy) -> dict[str, Any]:
    """Serialise a RetryPolicy back to a plain dict (e.g. for JSON output)."""
    return {
        "max_attempts": policy.max_attempts,
        "backoff_seconds": policy.backoff_seconds,
        "backoff_multiplier": policy.backoff_multiplier,
    }


def describe_policy(policy: RetryPolicy) -> str:
    """Return a human-readable one-liner describing the policy."""
    return (
        f"RetryPolicy: max_attempts={policy.max_attempts}, "
        f"backoff={policy.backoff_seconds}s, "
        f"multiplier={policy.backoff_multiplier}x"
    )
