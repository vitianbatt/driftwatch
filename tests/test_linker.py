"""Tests for driftwatch.linker."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.linker import DependencyMap, LinkerError, LinkedResult, link_results


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, diffs=None) -> DriftResult:
    return DriftResult(service=service, diffs=diffs or [])


def _diff(field: str) -> FieldDiff:
    return FieldDiff(field=field, expected="a", actual="b", kind="changed")


# ---------------------------------------------------------------------------
# DependencyMap
# ---------------------------------------------------------------------------

class TestDependencyMap:
    def test_valid_map_created(self):
        dm = DependencyMap(deps={"api": ["auth", "db"]})
        assert dm.dependencies_of("api") == ["auth", "db"]

    def test_missing_service_returns_empty(self):
        dm = DependencyMap(deps={})
        assert dm.dependencies_of("unknown") == []

    def test_none_deps_raises(self):
        with pytest.raises(LinkerError, match="None"):
            DependencyMap(deps=None)

    def test_empty_service_key_raises(self):
        with pytest.raises(LinkerError, match="empty"):
            DependencyMap(deps={"": ["auth"]})

    def test_whitespace_service_key_raises(self):
        with pytest.raises(LinkerError, match="whitespace"):
            DependencyMap(deps={"  ": ["auth"]})

    def test_non_list_deps_raises(self):
        with pytest.raises(LinkerError, match="list"):
            DependencyMap(deps={"api": "auth"})


# ---------------------------------------------------------------------------
# LinkedResult
# ---------------------------------------------------------------------------

class TestLinkedResult:
    def test_has_upstream_drift_true(self):
        lr = LinkedResult(result=_make("api"), dependencies=["auth"])
        assert lr.has_upstream_drift(["auth"]) is True

    def test_has_upstream_drift_false(self):
        lr = LinkedResult(result=_make("api"), dependencies=["auth"])
        assert lr.has_upstream_drift(["db"]) is False

    def test_has_upstream_drift_empty_deps(self):
        lr = LinkedResult(result=_make("api"), dependencies=[])
        assert lr.has_upstream_drift(["auth"]) is False

    def test_to_dict_keys(self):
        lr = LinkedResult(result=_make("api"), dependencies=["auth"], affected_by=["auth"])
        d = lr.to_dict()
        assert set(d.keys()) == {"service", "has_drift", "dependencies", "affected_by"}

    def test_to_dict_values(self):
        lr = LinkedResult(result=_make("api", [_diff("x")]), dependencies=["auth"], affected_by=["auth"])
        d = lr.to_dict()
        assert d["service"] == "api"
        assert d["has_drift"] is True
        assert d["dependencies"] == ["auth"]
        assert d["affected_by"] == ["auth"]


# ---------------------------------------------------------------------------
# link_results
# ---------------------------------------------------------------------------

class TestLinkResults:
    def test_none_results_raises(self):
        dm = DependencyMap(deps={})
        with pytest.raises(LinkerError, match="results"):
            link_results(None, dm)

    def test_none_dep_map_raises(self):
        with pytest.raises(LinkerError, match="dep_map"):
            link_results([], None)

    def test_empty_results_returns_empty(self):
        dm = DependencyMap(deps={})
        assert link_results([], dm) == []

    def test_clean_result_has_no_affected_by(self):
        dm = DependencyMap(deps={"api": ["auth"]})
        results = [_make("api"), _make("auth")]
        linked = link_results(results, dm)
        api_lr = next(lr for lr in linked if lr.result.service == "api")
        assert api_lr.affected_by == []

    def test_drifted_dep_appears_in_affected_by(self):
        dm = DependencyMap(deps={"api": ["auth"]})
        results = [_make("api"), _make("auth", [_diff("token")])]
        linked = link_results(results, dm)
        api_lr = next(lr for lr in linked if lr.result.service == "api")
        assert "auth" in api_lr.affected_by

    def test_service_without_deps_has_empty_dependencies(self):
        dm = DependencyMap(deps={})
        results = [_make("orphan")]
        linked = link_results(results, dm)
        assert linked[0].dependencies == []

    def test_all_results_returned(self):
        dm = DependencyMap(deps={"a": ["b"], "b": []})
        results = [_make("a"), _make("b")]
        linked = link_results(results, dm)
        assert len(linked) == 2
