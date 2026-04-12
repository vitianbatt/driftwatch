"""Unit tests for driftwatch.tagger_cli."""
from __future__ import annotations

import json
import textwrap

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.tagger_cli import (
    results_from_json,
    report_to_json,
    run_tagger,
    tag_map_from_yaml,
)
from driftwatch.tagging import TaggedResult


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [FieldDiff(field=f, expected="x", actual="y") for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# results_from_json
# ---------------------------------------------------------------------------

class TestResultsFromJson:
    def test_clean_result_parsed(self):
        raw = json.dumps([{"service": "auth", "diffs": []}])
        results = results_from_json(raw)
        assert len(results) == 1
        assert results[0].service == "auth"
        assert results[0].diffs == []

    def test_drift_result_diffs_parsed(self):
        raw = json.dumps([
            {"service": "api", "diffs": [{"field": "replicas", "expected": 3, "actual": 1}]}
        ])
        results = results_from_json(raw)
        assert results[0].diffs[0].field == "replicas"

    def test_multiple_results_parsed(self):
        raw = json.dumps([
            {"service": "a", "diffs": []},
            {"service": "b", "diffs": []},
        ])
        assert len(results_from_json(raw)) == 2


# ---------------------------------------------------------------------------
# report_to_json
# ---------------------------------------------------------------------------

class TestReportToJson:
    def test_output_is_valid_json(self):
        tagged = [TaggedResult(result=_make("svc"), tags={"team:a"})]
        out = report_to_json(tagged)
        parsed = json.loads(out)
        assert isinstance(parsed, list)

    def test_tags_are_sorted(self):
        tagged = [TaggedResult(result=_make("svc"), tags={"z", "a", "m"})]
        parsed = json.loads(report_to_json(tagged))
        assert parsed[0]["tags"] == ["a", "m", "z"]

    def test_drift_fields_listed(self):
        tagged = [TaggedResult(result=_make("svc", ["cpu", "mem"]), tags=set())]
        parsed = json.loads(report_to_json(tagged))
        assert set(parsed[0]["drift_fields"]) == {"cpu", "mem"}


# ---------------------------------------------------------------------------
# run_tagger (integration-style, uses tmp file)
# ---------------------------------------------------------------------------

class TestRunTagger:
    def test_no_filter_returns_all(self, tmp_path):
        tag_file = tmp_path / "tags.yaml"
        tag_file.write_text(textwrap.dedent("""\
            auth: [team:identity]
            api: [team:platform]
        """))
        raw = json.dumps([
            {"service": "auth", "diffs": []},
            {"service": "api", "diffs": []},
        ])
        out = json.loads(run_tagger(raw, str(tag_file)))
        assert len(out) == 2

    def test_filter_narrows_results(self, tmp_path):
        tag_file = tmp_path / "tags.yaml"
        tag_file.write_text(textwrap.dedent("""\
            auth: [team:identity]
            api: [team:platform]
        """))
        raw = json.dumps([
            {"service": "auth", "diffs": []},
            {"service": "api", "diffs": []},
        ])
        out = json.loads(run_tagger(raw, str(tag_file), filter_tag="team:identity"))
        assert len(out) == 1
        assert out[0]["service"] == "auth"
