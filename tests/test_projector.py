"""Tests for driftwatch.projector."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.projector import (
    ProjectorError,
    ProjectedField,
    ProjectedResult,
    project_results,
)


def _diff(field, expected=None, actual=None):
    return FieldDiff(field=field, expected=expected, actual=actual)


def _make(service, diffs=None):
    return DriftResult(service=service, diffs=diffs or [])


class TestProjectedField:
    def test_to_dict_keys(self):
        pf = ProjectedField(name="port", expected="8080", actual="9090", diff_type="changed")
        d = pf.to_dict()
        assert set(d.keys()) == {"name", "expected", "actual", "diff_type"}

    def test_to_dict_values(self):
        pf = ProjectedField(name="port", expected="8080", actual=None, diff_type="missing")
        d = pf.to_dict()
        assert d["name"] == "port"
        assert d["expected"] == "8080"
        assert d["actual"] is None
        assert d["diff_type"] == "missing"


class TestProjectedResult:
    def test_has_drift_false_when_no_fields(self):
        pr = ProjectedResult(service="svc")
        assert pr.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        pf = ProjectedField(name="x", expected="1", actual="2", diff_type="changed")
        pr = ProjectedResult(service="svc", fields=[pf])
        assert pr.has_drift() is True

    def test_field_names_returns_list(self):
        pf1 = ProjectedField(name="a", expected=None, actual="v", diff_type="extra")
        pf2 = ProjectedField(name="b", expected="v", actual=None, diff_type="missing")
        pr = ProjectedResult(service="svc", fields=[pf1, pf2])
        assert pr.field_names() == ["a", "b"]

    def test_to_dict_contains_all_keys(self):
        pr = ProjectedResult(service="svc")
        d = pr.to_dict()
        assert "service" in d
        assert "has_drift" in d
        assert "fields" in d


class TestProjectResults:
    def test_none_raises(self):
        with pytest.raises(ProjectorError):
            project_results(None)

    def test_empty_list_returns_empty(self):
        assert project_results([]) == []

    def test_clean_result_projects_no_fields(self):
        result = project_results([_make("auth")])
        assert len(result) == 1
        assert result[0].service == "auth"
        assert result[0].has_drift() is False

    def test_missing_diff_projects_correctly(self):
        r = _make("svc", diffs=[_diff("timeout", expected="30", actual=None)])
        projected = project_results([r])
        pf = projected[0].fields[0]
        assert pf.diff_type == "missing"
        assert pf.name == "timeout"
        assert pf.expected == "30"
        assert pf.actual is None

    def test_extra_diff_projects_correctly(self):
        r = _make("svc", diffs=[_diff("debug", expected=None, actual="true")])
        projected = project_results([r])
        pf = projected[0].fields[0]
        assert pf.diff_type == "extra"

    def test_changed_diff_projects_correctly(self):
        r = _make("svc", diffs=[_diff("replicas", expected="3", actual="1")])
        projected = project_results([r])
        pf = projected[0].fields[0]
        assert pf.diff_type == "changed"
        assert pf.expected == "3"
        assert pf.actual == "1"

    def test_multiple_results_preserved(self):
        results = [_make("a"), _make("b"), _make("c")]
        projected = project_results(results)
        assert [p.service for p in projected] == ["a", "b", "c"]
