"""Tests for driftwatch.scorer_exporter."""

from __future__ import annotations

import json
import pytest

from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult
from driftwatch.scorer_exporter import (
    ExportFormat,
    ScorerExporterError,
    export_csv,
    export_jsonl,
    export_scored_results,
)


def _diff(field: str) -> FieldDiff:
    return FieldDiff(field=field, expected="a", actual="b", kind="changed")


def _make(service: str, score: float = 0.0, diffs=None) -> ScoredResult:
    return ScoredResult(
        service=service,
        score=score,
        has_drift=bool(diffs),
        diffs=diffs or [],
    )


class TestExportJsonl:
    def test_empty_returns_empty_string(self):
        assert export_jsonl([]) == ""

    def test_none_raises(self):
        with pytest.raises(ScorerExporterError):
            export_jsonl(None)  # type: ignore

    def test_single_result_is_valid_json(self):
        r = _make("auth", score=5.0, diffs=[_diff("replicas")])
        line = export_jsonl([r])
        obj = json.loads(line)
        assert obj["service"] == "auth"
        assert obj["score"] == 5.0
        assert obj["has_drift"] is True
        assert "replicas" in obj["drift_fields"]

    def test_multiple_results_produce_multiple_lines(self):
        results = [_make("svc-a"), _make("svc-b")]
        output = export_jsonl(results)
        lines = output.splitlines()
        assert len(lines) == 2

    def test_clean_result_has_empty_drift_fields(self):
        r = _make("clean")
        obj = json.loads(export_jsonl([r]))
        assert obj["drift_fields"] == []


class TestExportCsv:
    def test_empty_returns_header_only(self):
        output = export_csv([])
        assert output.startswith("service,score,has_drift,drift_fields")
        assert len(output.splitlines()) == 1

    def test_none_raises(self):
        with pytest.raises(ScorerExporterError):
            export_csv(None)  # type: ignore

    def test_single_result_row_values(self):
        r = _make("payments", score=3.0, diffs=[_diff("timeout"), _diff("replicas")])
        lines = export_csv([r]).splitlines()
        assert len(lines) == 2
        assert "payments" in lines[1]
        assert "3.0" in lines[1]

    def test_drift_fields_pipe_separated(self):
        r = _make("svc", diffs=[_diff("a"), _diff("b")])
        row = export_csv([r]).splitlines()[1]
        assert "a|b" in row


class TestExportScoredResults:
    def test_jsonl_format_dispatches_correctly(self):
        r = _make("x", score=1.0)
        out = export_scored_results([r], ExportFormat.JSONL)
        assert json.loads(out)["service"] == "x"

    def test_csv_format_dispatches_correctly(self):
        r = _make("y", score=2.0)
        out = export_scored_results([r], ExportFormat.CSV)
        assert "y" in out

    def test_default_format_is_jsonl(self):
        r = _make("z")
        out = export_scored_results([r])
        obj = json.loads(out)
        assert obj["service"] == "z"
