"""Integration tests: load RetryPolicy from fixture YAML."""

from __future__ import annotations

import pathlib

import pytest
import yaml

from driftwatch.retrier import RetryPolicy, RetrierError, with_retry

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_retry_policy.yaml"


@pytest.fixture()
def policy_from_fixture() -> RetryPolicy:
    data = yaml.safe_load(FIXTURE.read_text())
    return RetryPolicy(
        max_attempts=data["max_attempts"],
        backoff_seconds=data["backoff_seconds"],
        backoff_multiplier=data["backoff_multiplier"],
    )


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_fixture_has_expected_keys(self):
        data = yaml.safe_load(FIXTURE.read_text())
        assert "max_attempts" in data
        assert "backoff_seconds" in data
        assert "backoff_multiplier" in data


class TestPolicyFromFixture:
    def test_max_attempts_is_four(self, policy_from_fixture: RetryPolicy):
        assert policy_from_fixture.max_attempts == 4

    def test_backoff_is_half_second(self, policy_from_fixture: RetryPolicy):
        assert policy_from_fixture.backoff_seconds == 0.5

    def test_multiplier_is_two(self, policy_from_fixture: RetryPolicy):
        assert policy_from_fixture.backoff_multiplier == 2.0

    def test_succeeds_with_fixture_policy(self, policy_from_fixture: RetryPolicy):
        result = with_retry(lambda: "live", policy_from_fixture, _sleep=lambda _: None)
        assert result.value == "live"
        assert result.succeeded is True

    def test_exhausts_fixture_policy(self, policy_from_fixture: RetryPolicy):
        with pytest.raises(RetrierError, match="4 attempt"):
            with_retry(
                lambda: (_ for _ in ()).throw(OSError("down")),
                policy_from_fixture,
                _sleep=lambda _: None,
            )
