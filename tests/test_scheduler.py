"""Tests for driftwatch.scheduler."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, call

import pytest

from driftwatch.scheduler import ScheduleConfig, SchedulerError, run_scheduled


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(drift: bool) -> MagicMock:
    r = MagicMock()
    r.has_drift = drift
    return r


# ---------------------------------------------------------------------------
# ScheduleConfig validation
# ---------------------------------------------------------------------------

class TestScheduleConfig:
    def test_valid_config_created(self):
        cfg = ScheduleConfig(interval_seconds=10, max_runs=5)
        assert cfg.interval_seconds == 10
        assert cfg.max_runs == 5
        assert cfg.tags == []

    def test_zero_interval_raises(self):
        with pytest.raises(SchedulerError, match="interval_seconds"):
            ScheduleConfig(interval_seconds=0)

    def test_negative_interval_raises(self):
        with pytest.raises(SchedulerError, match="interval_seconds"):
            ScheduleConfig(interval_seconds=-5)

    def test_zero_max_runs_raises(self):
        with pytest.raises(SchedulerError, match="max_runs"):
            ScheduleConfig(interval_seconds=10, max_runs=0)

    def test_none_max_runs_is_valid(self):
        cfg = ScheduleConfig(interval_seconds=60, max_runs=None)
        assert cfg.max_runs is None

    def test_tags_stored(self):
        cfg = ScheduleConfig(interval_seconds=30, tags=["prod", "api"])
        assert cfg.tags == ["prod", "api"]


# ---------------------------------------------------------------------------
# run_scheduled
# ---------------------------------------------------------------------------

class TestRunScheduled:
    def _no_sleep(self, _seconds: float) -> None:  # noqa: D401
        """Sleep stub that does nothing."""

    def test_runs_correct_number_of_times(self):
        check_fn = MagicMock(return_value=[])
        cfg = ScheduleConfig(interval_seconds=1, max_runs=3)
        total = run_scheduled(check_fn, cfg, sleep_fn=self._no_sleep)
        assert total == 3
        assert check_fn.call_count == 3

    def test_on_drift_callback_called_for_drifted_results(self):
        drifted = _make_result(True)
        clean = _make_result(False)
        check_fn = MagicMock(return_value=[drifted, clean])
        on_drift = MagicMock()
        cfg = ScheduleConfig(interval_seconds=1, max_runs=2, on_drift=on_drift)
        run_scheduled(check_fn, cfg, sleep_fn=self._no_sleep)
        assert on_drift.call_count == 2
        on_drift.assert_called_with([drifted])

    def test_on_drift_not_called_when_no_drift(self):
        check_fn = MagicMock(return_value=[_make_result(False)])
        on_drift = MagicMock()
        cfg = ScheduleConfig(interval_seconds=1, max_runs=2, on_drift=on_drift)
        run_scheduled(check_fn, cfg, sleep_fn=self._no_sleep)
        on_drift.assert_not_called()

    def test_on_error_callback_called_on_exception(self):
        check_fn = MagicMock(side_effect=RuntimeError("boom"))
        on_error = MagicMock()
        cfg = ScheduleConfig(interval_seconds=1, max_runs=2, on_error=on_error)
        total = run_scheduled(check_fn, cfg, sleep_fn=self._no_sleep)
        assert total == 0
        assert on_error.call_count == 2

    def test_sleep_called_between_runs_not_after_last(self):
        sleep_fn = MagicMock()
        check_fn = MagicMock(return_value=[])
        cfg = ScheduleConfig(interval_seconds=5, max_runs=3)
        run_scheduled(check_fn, cfg, sleep_fn=sleep_fn)
        # sleep should be called max_runs-1 times
        assert sleep_fn.call_count == 2
        sleep_fn.assert_called_with(5)
