"""Tests for driftwatch/stamper_cli.py."""
import json

import pytest

from driftwatch.stamper_cli import results_from_json, report_to_json, run_stamper
from driftwatch.stamper import StampReport, StampedResult


def _raw(service: str, fields=None) -> dict:
    return {"service": service, "drifted_fields": fields or []}


# ---------------------------------------------------------------------------
# results_from_json
# ---------------------------------------------------------------------------

class TestResultsFromJson:
    def test_clean_result_parsed(self):
        results = results_from_json([_raw("svc")])
        assert len(results) == 1
        assert results[0].service == "svc"
        assert results[0].drifted_fields == []

    def test_drift_result_diffs_parsed(self):
        results = results_from_json([_raw("svc", ["env", "replicas"])])
        assert results[0].drifted_fields == ["env", "replicas"]

    def test_multiple_results_parsed(self):
        raw = [_raw("a"), _raw("b", ["x"]), _raw("c")]
        results = results_from_json(raw)
        assert len(results) == 3
        assert results[1].drifted_fields == ["x"]

    def test_empty_list_returns_empty(self):
        assert results_from_json([]) == []


# ---------------------------------------------------------------------------
# report_to_json
# ---------------------------------------------------------------------------

class TestReportToJson:
    def test_output_is_valid_json(self):
        report = StampReport(
            results=[StampedResult("svc", [], "v1")]
        )
        out = report_to_json(report)
        parsed = json.loads(out)
        assert "summary" in parsed
        assert "results" in parsed

    def test_results_list_length(self):
        report = StampReport(
            results=[
                StampedResult("a", [], "v1"),
                StampedResult("b", ["f"], "v1"),
            ]
        )
        parsed = json.loads(report_to_json(report))
        assert len(parsed["results"]) == 2

    def test_stamp_present_in_output(self):
        report = StampReport(results=[StampedResult("svc", [], "sha-abc")])
        parsed = json.loads(report_to_json(report))
        assert parsed["results"][0]["stamp"] == "sha-abc"


# ---------------------------------------------------------------------------
# run_stamper
# ---------------------------------------------------------------------------

def test_run_stamper_returns_json_string():
    out = run_stamper([_raw("svc")], stamp="v1")
    parsed = json.loads(out)
    assert parsed["results"][0]["service"] == "svc"


def test_run_stamper_source_propagated():
    out = run_stamper([_raw("svc")], stamp="v2", source="ci")
    parsed = json.loads(out)
    assert parsed["results"][0]["source"] == "ci"


def test_run_stamper_empty_stamp_raises():
    from driftwatch.stamper import StamperError
    with pytest.raises(StamperError):
        run_stamper([_raw("svc")], stamp="")
