"""Tests for driftwatch/extractor.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.extractor import (
    ExtractorError,
    ExtractedResult,
    ExtractionReport,
    extract_fields,
)


def _diff(field: str, kind: str = "changed", expected="v1", actual="v2") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=expected, actual=actual)


def _make(service: str, *diffs: FieldDiff) -> DriftResult:
    return DriftResult(service=service, diffs=list(diffs))


# ---------------------------------------------------------------------------
# ExtractedResult
# ---------------------------------------------------------------------------

class TestExtractedResult:
    def test_has_drift_false_when_empty(self):
        r = ExtractedResult(service="svc")
        assert r.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        r = ExtractedResult(service="svc", extracted={"timeout": [_diff("timeout")]})
        assert r.has_drift() is True

    def test_field_names_sorted(self):
        r = ExtractedResult(
            service="svc",
            extracted={"z_field": [], "a_field": []},
        )
        assert r.field_names() == ["a_field", "z_field"]

    def test_to_dict_contains_expected_keys(self):
        r = ExtractedResult(service="svc")
        d = r.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "fields"}

    def test_to_dict_fields_stringified(self):
        fd = _diff("timeout")
        r = ExtractedResult(service="svc", extracted={"timeout": [fd]})
        d = r.to_dict()
        assert isinstance(d["fields"]["timeout"][0], str)


# ---------------------------------------------------------------------------
# ExtractionReport
# ---------------------------------------------------------------------------

class TestExtractionReport:
    def test_len_reflects_results(self):
        report = ExtractionReport(results=[ExtractedResult(service="a"), ExtractedResult(service="b")])
        assert len(report) == 2

    def test_service_names_in_order(self):
        report = ExtractionReport(results=[ExtractedResult(service="a"), ExtractedResult(service="b")])
        assert report.service_names() == ["a", "b"]

    def test_get_returns_correct_result(self):
        r = ExtractedResult(service="auth")
        report = ExtractionReport(results=[r])
        assert report.get("auth") is r

    def test_get_missing_returns_none(self):
        report = ExtractionReport(results=[])
        assert report.get("missing") is None


# ---------------------------------------------------------------------------
# extract_fields
# ---------------------------------------------------------------------------

def test_none_results_raises():
    with pytest.raises(ExtractorError, match="results"):
        extract_fields(None, ["timeout"])


def test_none_fields_raises():
    with pytest.raises(ExtractorError, match="fields list"):
        extract_fields([], None)


def test_empty_fields_raises():
    with pytest.raises(ExtractorError, match="empty"):
        extract_fields([], [])


def test_blank_field_name_raises():
    with pytest.raises(ExtractorError, match="blank"):
        extract_fields([], ["  "])


def test_empty_results_returns_empty_report():
    report = extract_fields([], ["timeout"])
    assert len(report) == 0


def test_matching_field_extracted():
    result = _make("auth", _diff("timeout"), _diff("retries"))
    report = extract_fields([result], ["timeout"])
    extracted = report.get("auth")
    assert extracted is not None
    assert "timeout" in extracted.extracted
    assert "retries" not in extracted.extracted


def test_no_matching_fields_gives_empty_extracted():
    result = _make("auth", _diff("memory"))
    report = extract_fields([result], ["timeout"])
    extracted = report.get("auth")
    assert extracted.has_drift() is False


def test_multiple_services_processed_independently():
    r1 = _make("svc-a", _diff("timeout"))
    r2 = _make("svc-b", _diff("replicas"))
    report = extract_fields([r1, r2], ["timeout"])
    assert report.get("svc-a").has_drift() is True
    assert report.get("svc-b").has_drift() is False


def test_field_names_stripped():
    result = _make("auth", _diff("timeout"))
    report = extract_fields([result], [" timeout "])
    extracted = report.get("auth")
    assert "timeout" in extracted.extracted
