"""Retry logic for transient failures when fetching live configs."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, TypeVar

T = TypeVar("T")


class RetrierError(Exception):
    """Raised when all retry attempts are exhausted."""


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: (Exception,)
    )

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise RetrierError("max_attempts must be >= 1")
        if self.backoff_seconds < 0:
            raise RetrierError("backoff_seconds must be >= 0")
        if self.backoff_multiplier < 1.0:
            raise RetrierError("backoff_multiplier must be >= 1.0")


@dataclass
class RetryResult:
    value: object
    attempts: int
    succeeded: bool

    def summary(self) -> str:
        status = "succeeded" if self.succeeded else "failed"
        return f"Retry {status} after {self.attempts} attempt(s)."


def with_retry(
    fn: Callable[[], T],
    policy: RetryPolicy | None = None,
    *,
    _sleep: Callable[[float], None] = time.sleep,
) -> RetryResult:
    """Call *fn* up to policy.max_attempts times, returning a RetryResult.

    Raises RetrierError if all attempts fail.
    """
    if policy is None:
        policy = RetryPolicy()

    delay = policy.backoff_seconds
    last_exc: Exception | None = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            value = fn()
            return RetryResult(value=value, attempts=attempt, succeeded=True)
        except policy.exceptions as exc:  # type: ignore[misc]
            last_exc = exc
            if attempt < policy.max_attempts:
                _sleep(delay)
                delay *= policy.backoff_multiplier

    raise RetrierError(
        f"All {policy.max_attempts} attempt(s) failed. "
        f"Last error: {last_exc}"
    ) from last_exc
