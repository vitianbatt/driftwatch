"""Tests for driftwatch.differ module."""

from __future__ import annotations

import pytest

from driftwatch.differ import DiffError, FieldDiff, deep_diff


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPEC = {
    "version": "2.1.0",
    "replicas": 3,
    "env": {"LOG_LEVEL": "INFO", "DB_HOST": "db.internal"},
}


# ---------------------------------------------------------------------------
# FieldDiff.__str__
# ---------------------------------------------------------------------------

class TestFieldDiffStr:
    def test_missing_str(self):
        d = FieldDiff(key="replicas", expected=3, actual=None, diff_type="missing")
        assert "missing" in str(d).lower() or "not present" in str(d)

    def test_extra_str(self):
        d = FieldDiff(key="debug", expected=None, actual=True, diff_type="extra")
        assert "not in spec" in str(d)

    def test_changed_str(self):
        d = FieldDiff(key="version", expected="2.1.0", actual="2.0.0", diff_type="changed")
        assert "2.1.0" in str(d) and "2.0.0" in str(d)


# ---------------------------------------------------------------------------
# deep_diff
# ---------------------------------------------------------------------------

class TestDeepDiff:
    def test_identical_dicts_return_empty(self):
        assert deep_diff(SPEC, SPEC.copy()) == []

    def test_detects_missing_key(self):
        live = {"version": "2.1.0"}  # missing replicas and env
        diffs = deep_diff(SPEC, live)
        keys = {d.key for d in diffs}
        assert "replicas" in keys

    def test_detects_changed_value(self):
        live = {**SPEC, "replicas": 1}
        diffs = deep_diff(SPEC, live)
        changed = [d for d in diffs if d.diff_type == "changed"]
        assert len(changed) == 1
        assert changed[0].key == "replicas"
        assert changed[0].expected == 3
        assert changed[0].actual == 1

    def test_detects_extra_key(self):
        live = {**SPEC, "debug": True}
        diffs = deep_diff(SPEC, live)
        extra = [d for d in diffs if d.diff_type == "extra"]
        assert any(d.key == "debug" for d in extra)

    def test_ignore_extra_suppresses_extra_keys(self):
        live = {**SPEC, "debug": True}
        diffs = deep_diff(SPEC, live, ignore_extra=True)
        assert all(d.diff_type != "extra" for d in diffs)

    def test_nested_diff_uses_dotted_path(self):
        live = {**SPEC, "env": {"LOG_LEVEL": "DEBUG", "DB_HOST": "db.internal"}}
        diffs = deep_diff(SPEC, live)
        assert any(d.key == "env.LOG_LEVEL" for d in diffs)

    def test_nested_missing_key_reported(self):
        live = {**SPEC, "env": {"LOG_LEVEL": "INFO"}}  # DB_HOST missing
        diffs = deep_diff(SPEC, live)
        assert any(d.key == "env.DB_HOST" and d.diff_type == "missing" for d in diffs)

    def test_raises_diff_error_on_non_dict_spec(self):
        with pytest.raises(DiffError):
            deep_diff("not-a-dict", {"key": "value"})

    def test_raises_diff_error_on_non_dict_live(self):
        with pytest.raises(DiffError):
            deep_diff({"key": "value"}, ["not", "a", "dict"])

    def test_multiple_missing_keys_all_reported(self):
        """All missing top-level keys should appear in the diff results."""
        live = {}  # everything is missing
        diffs = deep_diff(SPEC, live)
        keys = {d.key for d in diffs}
        assert "version" in keys
        assert "replicas" in keys
        assert "env" in keys or any(k.startswith("env.") for k in keys)
