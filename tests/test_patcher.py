"""Tests for driftwatch.patcher."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.patcher import (
    PatcherError,
    PatchReport,
    PatchSuggestion,
    generate_patches,
)


def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field, expected, actual):
    return FieldDiff(field=field, expected=expected, actual=actual)


# ---------------------------------------------------------------------------
# PatchSuggestion
# ---------------------------------------------------------------------------
class TestPatchSuggestion:
    def test_set_action_describe(self):
        s = PatchSuggestion("svc", "replicas", "set", expected=3, actual=None)
        assert "SET" in s.describe()
        assert "replicas" in s.describe()

    def test_remove_action_describe(self):
        s = PatchSuggestion("svc", "debug", "remove", expected=None, actual=True)
        assert "REMOVE" in s.describe()

    def test_update_action_describe(self):
        s = PatchSuggestion("svc", "timeout", "update", expected=30, actual=10)
        assert "UPDATE" in s.describe()
        assert "30" in s.describe()

    def test_to_dict_keys(self):
        s = PatchSuggestion("svc", "key", "set", expected=1, actual=None)
        d = s.to_dict()
        assert set(d) == {"service", "field", "action", "expected", "actual"}


# ---------------------------------------------------------------------------
# PatchReport
# ---------------------------------------------------------------------------
class TestPatchReport:
    def test_empty_report_no_suggestions(self):
        r = PatchReport()
        assert not r.has_suggestions()
        assert r.total == 0

    def test_summary_no_suggestions(self):
        assert PatchReport().summary() == "No patches required."

    def test_summary_with_suggestions(self):
        s = PatchSuggestion("svc", "k", "set", expected=1, actual=None)
        r = PatchReport(suggestions=[s])
        text = r.summary()
        assert "1 patch suggestion" in text
        assert "SET" in text


# ---------------------------------------------------------------------------
# generate_patches
# ---------------------------------------------------------------------------
class TestGeneratePatches:
    def test_none_raises(self):
        with pytest.raises(PatcherError):
            generate_patches(None)

    def test_empty_list_returns_empty_report(self):
        report = generate_patches([])
        assert not report.has_suggestions()

    def test_clean_result_no_suggestions(self):
        report = generate_patches([_make("auth")])
        assert report.total == 0

    def test_missing_field_produces_set_action(self):
        result = _make("auth", [_diff("replicas", 3, None)])
        report = generate_patches([result])
        assert report.total == 1
        assert report.suggestions[0].action == "set"
        assert report.suggestions[0].service == "auth"

    def test_extra_field_produces_remove_action(self):
        result = _make("auth", [_diff("debug", None, True)])
        report = generate_patches([result])
        assert report.suggestions[0].action == "remove"

    def test_changed_field_produces_update_action(self):
        result = _make("auth", [_diff("timeout", 30, 10)])
        report = generate_patches([result])
        assert report.suggestions[0].action == "update"

    def test_multiple_results_aggregated(self):
        r1 = _make("auth", [_diff("replicas", 2, None)])
        r2 = _make("billing", [_diff("timeout", 60, 30), _diff("debug", None, True)])
        report = generate_patches([r1, r2])
        assert report.total == 3
        services = [s.service for s in report.suggestions]
        assert "auth" in services
        assert "billing" in services
