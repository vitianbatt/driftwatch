"""Tests for driftwatch.retrier."""

from __future__ import annotations

import pytest

from driftwatch.retrier import (
    RetryPolicy,
    RetryResult,
    RetrierError,
    with_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _no_sleep(seconds: float) -> None:  # noqa: ARG001
    """Replace time.sleep so tests run instantly."""


# ---------------------------------------------------------------------------
# TestRetryPolicy
# ---------------------------------------------------------------------------

class TestRetryPolicy:
    def test_defaults_are_valid(self):
        p = RetryPolicy()
        assert p.max_attempts == 3
        assert p.backoff_seconds == 1.0
        assert p.backoff_multiplier == 2.0

    def test_zero_attempts_raises(self):
        with pytest.raises(RetrierError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_negative_backoff_raises(self):
        with pytest.raises(RetrierError, match="backoff_seconds"):
            RetryPolicy(backoff_seconds=-0.1)

    def test_multiplier_below_one_raises(self):
        with pytest.raises(RetrierError, match="backoff_multiplier"):
            RetryPolicy(backoff_multiplier=0.5)


# ---------------------------------------------------------------------------
# TestRetryResult
# ---------------------------------------------------------------------------

class TestRetryResult:
    def test_summary_succeeded(self):
        r = RetryResult(value=42, attempts=2, succeeded=True)
        assert "succeeded" in r.summary()
        assert "2" in r.summary()

    def test_summary_failed(self):
        r = RetryResult(value=None, attempts=3, succeeded=False)
        assert "failed" in r.summary()


# ---------------------------------------------------------------------------
# TestWithRetry
# ---------------------------------------------------------------------------

class TestWithRetry:
    def test_succeeds_on_first_attempt(self):
        result = with_retry(lambda: 99, _sleep=_no_sleep)
        assert result.value == 99
        assert result.attempts == 1
        assert result.succeeded is True

    def test_succeeds_after_transient_failures(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ConnectionError("boom")
            return "ok"

        policy = RetryPolicy(max_attempts=3, backoff_seconds=0)
        result = with_retry(flaky, policy, _sleep=_no_sleep)
        assert result.value == "ok"
        assert result.attempts == 3

    def test_raises_after_all_attempts_exhausted(self):
        policy = RetryPolicy(max_attempts=2, backoff_seconds=0)
        with pytest.raises(RetrierError, match="2 attempt"):
            with_retry(lambda: (_ for _ in ()).throw(RuntimeError("fail")), policy, _sleep=_no_sleep)

    def test_only_retries_specified_exceptions(self):
        policy = RetryPolicy(
            max_attempts=3,
            backoff_seconds=0,
            exceptions=(ValueError,),
        )
        # TypeError is NOT in the retry list — should propagate immediately
        with pytest.raises(TypeError):
            with_retry(lambda: (_ for _ in ()).throw(TypeError("wrong")), policy, _sleep=_no_sleep)

    def test_default_policy_used_when_none_passed(self):
        result = with_retry(lambda: "default", _sleep=_no_sleep)
        assert result.value == "default"

    def test_sleep_called_between_attempts(self):
        slept: list[float] = []
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise IOError("retry me")
            return "done"

        policy = RetryPolicy(max_attempts=3, backoff_seconds=0.5, backoff_multiplier=2.0)
        with_retry(flaky, policy, _sleep=slept.append)
        assert slept == [0.5, 1.0]
