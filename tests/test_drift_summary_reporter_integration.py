"""Integration tests for drift_summary_reporter using fixture data."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.drift_summary_reporter import (
    SummaryFormat,
    build_summary,
    generate_summary_report,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample_drift_summary_input.yaml"


def _load_fixture() -> list[DriftResult]:
    raw = yaml.safe_load(FIXTURE.read_text())
    results = []
    for item in raw:
        diffs = (
            [FieldDiff(field="replicas", expected=3, actual=1, diff_type="changed")]
            if item.get("drifted")
            else []
        )
        results.append(DriftResult(service=item["service"], diffs=diffs))
    return results


@pytest.fixture(scope="module")
def report():
    return build_summary(_load_fixture())


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_four_results_loaded(self, report):
        assert report.total == 4

    def test_two_drifted(self, report):
        assert report.drifted == 2

    def test_two_clean(self, report):
        assert report.clean == 2

    def test_drift_rate_is_fifty_percent(self, report):
        assert report.drift_rate == 0.5

    def test_drifted_services_sorted(self, report):
        assert report.drifted_services == ["auth-service", "gateway-service"]

    def test_text_report_includes_services(self, report):
        results = _load_fixture()
        text = generate_summary_report(results)
        assert "auth-service" in text
        assert "gateway-service" in text

    def test_json_report_parseable(self):
        import json
        results = _load_fixture()
        output = generate_summary_report(results, fmt=SummaryFormat.JSON)
        parsed = json.loads(output)
        assert parsed["total"] == 4
