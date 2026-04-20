"""Integration tests for windower using the YAML fixture."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.windower import WindowConfig, build_windows

FIXTURE = Path(__file__).parent / "fixtures" / "sample_windower_input.yaml"


def _load_fixture():
    with FIXTURE.open() as fh:
        return yaml.safe_load(fh)


def _results_from_fixture(data) -> list:
    out = []
    for entry in data["results"]:
        diffs = (
            [FieldDiff(field="env", expected="prod", actual="staging")]
            if entry["drifted"]
            else []
        )
        out.append(DriftResult(service=entry["service"], diffs=diffs))
    return out


@pytest.fixture(scope="module")
def report():
    data = _load_fixture()
    cfg_data = data.get("window_config", {})
    cfg = WindowConfig(
        size=cfg_data.get("size", 5),
        step=cfg_data.get("step", 1),
    )
    results = _results_from_fixture(data)
    return build_windows(results, cfg)


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_six_results_loaded(self, report):
        total = sum(len(w) for w in report.windows)
        # with size=3, step=2 over 6 items: windows at 0,2,4 → 3 windows
        # last window has 2 items
        assert total == 8  # 3+3+2

    def test_three_windows_created(self, report):
        assert report.total_windows() == 3

    def test_first_window_has_three_results(self, report):
        assert len(report.windows[0]) == 3

    def test_first_window_one_drifted(self, report):
        # auth(F), payment(T), notification(F) → 1 drift
        assert report.windows[0].drift_count() == 1

    def test_second_window_two_drifted(self, report):
        # notification(F), inventory(T), shipping(T) → 2 drifts
        assert report.windows[1].drift_count() == 2

    def test_peak_window_is_second(self, report):
        peak = report.peak_drift_window()
        assert peak is not None
        assert peak.drift_rate() >= 0.5

    def test_config_stored_on_report(self, report):
        assert report.config is not None
        assert report.config.size == 3
        assert report.config.step == 2
