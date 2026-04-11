"""Tests for driftwatch.annotator."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.annotator import (
    AnnotatedResult,
    AnnotatorError,
    annotate_results,
    filter_by_note,
)


def _make(service: str, drift_fields=None) -> DriftResult:
    return DriftResult(
        service=service,
        has_drift=bool(drift_fields),
        drift_fields=drift_fields or [],
    )


# ---------------------------------------------------------------------------
# AnnotatedResult
# ---------------------------------------------------------------------------

class TestAnnotatedResult:
    def test_has_notes_false_when_empty(self):
        ar = AnnotatedResult(result=_make("svc"))
        assert ar.has_notes() is False

    def test_has_notes_true_when_populated(self):
        ar = AnnotatedResult(result=_make("svc"), notes=["todo"])
        assert ar.has_notes() is True

    def test_to_dict_contains_expected_keys(self):
        ar = AnnotatedResult(result=_make("svc", ["env"]), notes=["check env"])
        d = ar.to_dict()
        assert d["service"] == "svc"
        assert d["has_drift"] is True
        assert d["drift_fields"] == ["env"]
        assert d["notes"] == ["check env"]

    def test_to_dict_notes_is_copy(self):
        notes = ["a"]
        ar = AnnotatedResult(result=_make("svc"), notes=notes)
        ar.to_dict()["notes"].append("b")
        assert ar.notes == ["a"]


# ---------------------------------------------------------------------------
# annotate_results
# ---------------------------------------------------------------------------

def test_empty_results_returns_empty():
    assert annotate_results([], {}) == []


def test_none_results_raises():
    with pytest.raises(AnnotatorError):
        annotate_results(None, {})


def test_none_note_map_raises():
    with pytest.raises(AnnotatorError):
        annotate_results([], None)


def test_service_with_notes_attached():
    results = [_make("auth")]
    note_map = {"auth": ["reviewed", "pending fix"]}
    out = annotate_results(results, note_map)
    assert len(out) == 1
    assert out[0].notes == ["reviewed", "pending fix"]


def test_service_without_entry_gets_empty_notes():
    results = [_make("billing")]
    out = annotate_results(results, {})
    assert out[0].notes == []


def test_multiple_services_annotated_independently():
    results = [_make("auth"), _make("billing"), _make("gateway")]
    note_map = {"auth": ["note-a"], "gateway": ["note-g"]}
    out = annotate_results(results, note_map)
    services = {a.result.service: a.notes for a in out}
    assert services["auth"] == ["note-a"]
    assert services["billing"] == []
    assert services["gateway"] == ["note-g"]


# ---------------------------------------------------------------------------
# filter_by_note
# ---------------------------------------------------------------------------

def test_filter_by_note_returns_matching():
    ar1 = AnnotatedResult(result=_make("auth"), notes=["needs review"])
    ar2 = AnnotatedResult(result=_make("billing"), notes=["all clear"])
    out = filter_by_note([ar1, ar2], "review")
    assert len(out) == 1
    assert out[0].result.service == "auth"


def test_filter_by_note_case_insensitive():
    ar = AnnotatedResult(result=_make("svc"), notes=["URGENT fix"])
    out = filter_by_note([ar], "urgent")
    assert len(out) == 1


def test_filter_by_note_no_match_returns_empty():
    ar = AnnotatedResult(result=_make("svc"), notes=["fine"])
    assert filter_by_note([ar], "critical") == []


def test_filter_by_note_none_list_raises():
    with pytest.raises(AnnotatorError):
        filter_by_note(None, "x")


def test_filter_by_note_empty_keyword_raises():
    with pytest.raises(AnnotatorError):
        filter_by_note([], "")


def test_filter_by_note_whitespace_keyword_raises():
    with pytest.raises(AnnotatorError):
        filter_by_note([], "   ")
