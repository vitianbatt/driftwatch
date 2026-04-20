"""Tests for driftwatch.windower."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.windower import (
    Window,
    WindowConfig,
    WindowerError,
    WindowedReport,
    build_windows,
)


def _make(service: str, drifted: bool = False) -> DriftResult:
    diffs = [FieldDiff(field="x", expected="a", actual="b")] if drifted else []
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# WindowConfig
# ---------------------------------------------------------------------------
class TestWindowConfig:
    def test_defaults_are_valid(self):
        cfg = WindowConfig()
        assert cfg.size == 5
        assert cfg.step == 1

    def test_zero_size_raises(self):
        with pytest.raises(WindowerError, match="size"):
            WindowConfig(size=0)

    def test_negative_size_raises(self):
        with pytest.raises(WindowerError, match="size"):
            WindowConfig(size=-1)

    def test_zero_step_raises(self):
        with pytest.raises(WindowerError, match="step"):
            WindowConfig(size=3, step=0)

    def test_step_exceeds_size_raises(self):
        with pytest.raises(WindowerError, match="step must not exceed size"):
            WindowConfig(size=3, step=5)

    def test_step_equal_to_size_is_valid(self):
        cfg = WindowConfig(size=4, step=4)
        assert cfg.step == 4


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------
class TestWindow:
    def test_len_empty(self):
        w = Window(index=0)
        assert len(w) == 0

    def test_drift_count_counts_drifted(self):
        w = Window(index=0, results=[_make("a", True), _make("b", False)])
        assert w.drift_count() == 1

    def test_drift_rate_empty_returns_zero(self):
        assert Window(index=0).drift_rate() == 0.0

    def test_drift_rate_all_drifted(self):
        w = Window(index=0, results=[_make("a", True), _make("b", True)])
        assert w.drift_rate() == 1.0

    def test_to_dict_contains_expected_keys(self):
        w = Window(index=2, results=[_make("svc", True)])
        d = w.to_dict()
        assert set(d.keys()) == {"index", "total", "drift_count", "drift_rate", "services"}

    def test_service_names_order_preserved(self):
        w = Window(index=0, results=[_make("alpha"), _make("beta")])
        assert w.service_names() == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# build_windows
# ---------------------------------------------------------------------------
class TestBuildWindows:
    def test_none_raises(self):
        with pytest.raises(WindowerError):
            build_windows(None)  # type: ignore[arg-type]

    def test_empty_list_returns_no_windows(self):
        report = build_windows([])
        assert report.total_windows() == 0

    def test_single_result_single_window(self):
        report = build_windows([_make("svc")])
        assert report.total_windows() == 1

    def test_window_size_respected(self):
        results = [_make(f"s{i}") for i in range(6)]
        report = build_windows(results, WindowConfig(size=3, step=3))
        assert report.total_windows() == 2
        assert len(report.windows[0]) == 3

    def test_sliding_step_produces_overlapping_windows(self):
        results = [_make(f"s{i}") for i in range(5)]
        report = build_windows(results, WindowConfig(size=3, step=1))
        # windows start at 0,1,2
        assert report.total_windows() == 3

    def test_peak_drift_window_none_when_empty(self):
        report = build_windows([])
        assert report.peak_drift_window() is None

    def test_peak_drift_window_returns_highest_rate(self):
        results = [
            _make("a", False), _make("b", False),
            _make("c", True),  _make("d", True),
        ]
        report = build_windows(results, WindowConfig(size=2, step=2))
        peak = report.peak_drift_window()
        assert peak is not None
        assert peak.drift_rate() == 1.0
