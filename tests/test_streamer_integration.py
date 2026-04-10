"""Integration tests for streamer using the sample fixture."""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.streamer import StreamConfig, stream_results

FIXTURE = Path("tests/fixtures/sample_stream_input.jsonl")


def _load_fixture() -> list[DriftResult]:
    results = []
    for line in FIXTURE.read_text().splitlines():
        if not line.strip():
            continue
        d = json.loads(line)
        results.append(
            DriftResult(
                service=d["service"],
                missing_keys=d["missing_keys"],
                extra_keys=d["extra_keys"],
                changed_keys=d["changed_keys"],
            )
        )
    return results


@pytest.fixture()
def results() -> list[DriftResult]:
    return _load_fixture()


class TestFixtureLoads:
    def test_fixture_exists(self):
        assert FIXTURE.exists()

    def test_three_results_loaded(self, results):
        assert len(results) == 3

    def test_first_result_is_clean(self, results):
        assert results[0].service == "auth"
        assert not results[0].has_drift

    def test_second_result_has_missing_key(self, results):
        assert results[1].service == "billing"
        assert "timeout" in results[1].missing_keys

    def test_third_result_has_extra_and_changed(self, results):
        r = results[2]
        assert "debug" in r.extra_keys
        assert "replicas" in r.changed_keys


class TestStreamIntegration:
    def test_jsonl_round_trip(self, results):
        buf = io.StringIO()
        count = stream_results(results, StreamConfig(out=buf))
        assert count == 3
        lines = buf.getvalue().strip().splitlines()
        parsed = [json.loads(l) for l in lines]
        services = [p["service"] for p in parsed]
        assert services == ["auth", "billing", "gateway"]

    def test_text_output_has_ok_and_drift(self, results):
        buf = io.StringIO()
        stream_results(results, StreamConfig(format="text", out=buf))
        output = buf.getvalue()
        assert "[OK]" in output
        assert "[DRIFT]" in output

    def test_drift_count_from_fixture(self, results):
        drifted = [r for r in results if r.has_drift]
        assert len(drifted) == 2
