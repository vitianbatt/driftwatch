"""Tests for driftwatch/renamer.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.renamer import (
    RenameMap,
    RenamedResult,
    RenamerError,
    rename_results,
)


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field_name: str, kind: str = "changed", expected="a", actual="b") -> FieldDiff:
    return FieldDiff(field_name=field_name, kind=kind, expected=expected, actual=actual)


# ---------------------------------------------------------------------------
# RenameMap
# ---------------------------------------------------------------------------

class TestRenameMap:
    def test_valid_map_created(self):
        rm = RenameMap(mappings={"old_key": "new_key"})
        assert rm.translate("old_key") == "new_key"

    def test_unmapped_key_returned_unchanged(self):
        rm = RenameMap(mappings={"x": "y"})
        assert rm.translate("unknown") == "unknown"

    def test_none_mappings_raises(self):
        with pytest.raises(RenamerError, match="mappings must not be None"):
            RenameMap(mappings=None)

    def test_empty_key_raises(self):
        with pytest.raises(RenamerError, match="mapping key"):
            RenameMap(mappings={"": "valid"})

    def test_whitespace_key_raises(self):
        with pytest.raises(RenamerError, match="mapping key"):
            RenameMap(mappings={"   ": "valid"})

    def test_empty_value_raises(self):
        with pytest.raises(RenamerError, match="mapping value"):
            RenameMap(mappings={"valid": ""})

    def test_whitespace_value_raises(self):
        with pytest.raises(RenamerError, match="mapping value"):
            RenameMap(mappings={"valid": "  "})


# ---------------------------------------------------------------------------
# rename_results
# ---------------------------------------------------------------------------

class TestRenameResults:
    def test_empty_results_returns_empty(self):
        rm = RenameMap(mappings={"old": "new"})
        assert rename_results([], rm) == []

    def test_none_results_raises(self):
        rm = RenameMap(mappings={})
        with pytest.raises(RenamerError, match="results must not be None"):
            rename_results(None, rm)  # type: ignore[arg-type]

    def test_none_map_raises(self):
        with pytest.raises(RenamerError, match="rename_map must not be None"):
            rename_results([], None)  # type: ignore[arg-type]

    def test_field_name_translated(self):
        rm = RenameMap(mappings={"mem_limit": "memory_limit"})
        result = _make("svc", [_diff("mem_limit")])
        renamed = rename_results([result], rm)
        assert renamed[0].diffs[0].field_name == "memory_limit"

    def test_unmapped_field_unchanged(self):
        rm = RenameMap(mappings={"other": "something"})
        result = _make("svc", [_diff("cpu_limit")])
        renamed = rename_results([result], rm)
        assert renamed[0].diffs[0].field_name == "cpu_limit"

    def test_service_name_preserved(self):
        rm = RenameMap(mappings={})
        result = _make("auth-service")
        renamed = rename_results([result], rm)
        assert renamed[0].service == "auth-service"

    def test_original_reference_stored(self):
        rm = RenameMap(mappings={})
        result = _make("svc")
        renamed = rename_results([result], rm)
        assert renamed[0].original is result

    def test_has_drift_true_when_diffs_present(self):
        rm = RenameMap(mappings={})
        result = _make("svc", [_diff("field")])
        renamed = rename_results([result], rm)
        assert renamed[0].has_drift() is True

    def test_has_drift_false_when_no_diffs(self):
        rm = RenameMap(mappings={})
        result = _make("svc")
        renamed = rename_results([result], rm)
        assert renamed[0].has_drift() is False

    def test_to_dict_structure(self):
        rm = RenameMap(mappings={"old": "new"})
        result = _make("svc", [_diff("old", kind="missing", expected="x", actual=None)])
        renamed = rename_results([result], rm)
        d = renamed[0].to_dict()
        assert d["service"] == "svc"
        assert d["has_drift"] is True
        assert d["diffs"][0]["field"] == "new"
        assert d["diffs"][0]["kind"] == "missing"

    def test_multiple_results_all_translated(self):
        rm = RenameMap(mappings={"a": "alpha", "b": "beta"})
        results = [
            _make("s1", [_diff("a")]),
            _make("s2", [_diff("b")]),
        ]
        renamed = rename_results(results, rm)
        assert renamed[0].diffs[0].field_name == "alpha"
        assert renamed[1].diffs[0].field_name == "beta"
