"""Tests for driftwatch.zipper."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.zipper import (
    ZipperError,
    ZippedField,
    ZippedResult,
    zip_result,
    zip_all,
)


def _result(service: str = "svc") -> DriftResult:
    return DriftResult(service=service, drifted_fields=[])


# ---------------------------------------------------------------------------
# ZippedField
# ---------------------------------------------------------------------------
class TestZippedField:
    def test_to_dict_keys(self):
        zf = ZippedField("timeout", 30, 60)
        d = zf.to_dict()
        assert set(d) == {"field", "spec", "live", "is_missing", "is_extra"}

    def test_to_dict_values(self):
        zf = ZippedField("replicas", 2, 3, is_missing=False, is_extra=False)
        d = zf.to_dict()
        assert d["field"] == "replicas"
        assert d["spec"] == 2
        assert d["live"] == 3

    def test_missing_flag(self):
        zf = ZippedField("key", "v", None, is_missing=True)
        assert zf.to_dict()["is_missing"] is True

    def test_extra_flag(self):
        zf = ZippedField("key", None, "v", is_extra=True)
        assert zf.to_dict()["is_extra"] is True


# ---------------------------------------------------------------------------
# ZippedResult
# ---------------------------------------------------------------------------
class TestZippedResult:
    def test_has_drift_false_when_values_match(self):
        zr = ZippedResult("svc", [ZippedField("k", 1, 1)])
        assert zr.has_drift() is False

    def test_has_drift_true_when_values_differ(self):
        zr = ZippedResult("svc", [ZippedField("k", 1, 2)])
        assert zr.has_drift() is True

    def test_has_drift_false_when_empty(self):
        zr = ZippedResult("svc", [])
        assert zr.has_drift() is False

    def test_to_dict_contains_all_keys(self):
        zr = ZippedResult("svc", [])
        d = zr.to_dict()
        assert "service" in d
        assert "has_drift" in d
        assert "fields" in d


# ---------------------------------------------------------------------------
# zip_result
# ---------------------------------------------------------------------------
class TestZipResult:
    def test_none_result_raises(self):
        with pytest.raises(ZipperError):
            zip_result(None, {}, {})

    def test_none_spec_raises(self):
        with pytest.raises(ZipperError):
            zip_result(_result(), None, {})

    def test_none_live_raises(self):
        with pytest.raises(ZipperError):
            zip_result(_result(), {}, None)

    def test_empty_dicts_returns_no_fields(self):
        zr = zip_result(_result(), {}, {})
        assert zr.fields == []

    def test_matching_keys_paired(self):
        zr = zip_result(_result(), {"timeout": 30}, {"timeout": 60})
        assert len(zr.fields) == 1
        assert zr.fields[0].spec_value == 30
        assert zr.fields[0].live_value == 60

    def test_missing_key_flagged(self):
        zr = zip_result(_result(), {"timeout": 30}, {})
        assert zr.fields[0].is_missing is True
        assert zr.fields[0].live_value is None

    def test_extra_key_flagged(self):
        zr = zip_result(_result(), {}, {"extra": "v"})
        assert zr.fields[0].is_extra is True
        assert zr.fields[0].spec_value is None

    def test_fields_sorted_alphabetically(self):
        spec = {"z": 1, "a": 2, "m": 3}
        live = {"z": 1, "a": 2, "m": 3}
        zr = zip_result(_result(), spec, live)
        names = [f.field_name for f in zr.fields]
        assert names == sorted(names)

    def test_service_name_preserved(self):
        zr = zip_result(_result("auth-service"), {}, {})
        assert zr.service == "auth-service"


# ---------------------------------------------------------------------------
# zip_all
# ---------------------------------------------------------------------------
class TestZipAll:
    def test_none_results_raises(self):
        with pytest.raises(ZipperError):
            zip_all(None, {}, {})

    def test_empty_results_returns_empty(self):
        assert zip_all([], {}, {}) == []

    def test_multiple_results_zipped(self):
        results = [_result("svc-a"), _result("svc-b")]
        specs = {"svc-a": {"k": 1}, "svc-b": {"k": 2}}
        lives = {"svc-a": {"k": 1}, "svc-b": {"k": 9}}
        zipped = zip_all(results, specs, lives)
        assert len(zipped) == 2
        assert zipped[0].service == "svc-a"
        assert zipped[1].has_drift() is True

    def test_missing_spec_defaults_to_empty(self):
        results = [_result("ghost")]
        zipped = zip_all(results, {}, {"ghost": {"k": "v"}})
        assert zipped[0].fields[0].is_extra is True
