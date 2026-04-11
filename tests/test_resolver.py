"""Tests for driftwatch.resolver."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.resolver import (
    OwnerMap,
    ResolvedResult,
    ResolverError,
    resolve_results,
    unowned,
)


def _make(service: str, drift_fields=None) -> DriftResult:
    return DriftResult(service=service, drift_fields=drift_fields or [])


class TestOwnerMap:
    def test_valid_map_created(self):
        om = OwnerMap({"auth": "team-a"})
        assert om.lookup("auth") == "team-a"

    def test_lookup_missing_returns_none(self):
        om = OwnerMap({"auth": "team-a"})
        assert om.lookup("unknown") is None

    def test_empty_service_key_raises(self):
        with pytest.raises(ResolverError):
            OwnerMap({"": "team-a"})

    def test_blank_owner_raises(self):
        with pytest.raises(ResolverError):
            OwnerMap({"auth": "   "})

    def test_non_dict_raises(self):
        with pytest.raises(ResolverError):
            OwnerMap(None)  # type: ignore


class TestResolvedResult:
    def test_has_owner_true(self):
        r = ResolvedResult(result=_make("auth"), owner="team-a")
        assert r.has_owner() is True

    def test_has_owner_false_when_none(self):
        r = ResolvedResult(result=_make("auth"), owner=None)
        assert r.has_owner() is False

    def test_to_dict_keys(self):
        r = ResolvedResult(result=_make("auth", ["timeout"]), owner="team-a")
        d = r.to_dict()
        assert d["service"] == "auth"
        assert d["owner"] == "team-a"
        assert d["has_drift"] is True
        assert "timeout" in d["drift_fields"]


def test_resolve_results_assigns_owners():
    results = [_make("auth"), _make("billing")]
    om = OwnerMap({"auth": "team-a", "billing": "team-b"})
    resolved = resolve_results(results, om)
    assert len(resolved) == 2
    assert resolved[0].owner == "team-a"
    assert resolved[1].owner == "team-b"


def test_resolve_results_none_for_unmapped():
    results = [_make("gateway")]
    om = OwnerMap({"auth": "team-a"})
    resolved = resolve_results(results, om)
    assert resolved[0].owner is None


def test_resolve_results_none_raises():
    with pytest.raises(ResolverError):
        resolve_results(None, OwnerMap({"auth": "team-a"}))  # type: ignore


def test_resolve_owner_map_none_raises():
    with pytest.raises(ResolverError):
        resolve_results([_make("auth")], None)  # type: ignore


def test_unowned_filters_correctly():
    results = [_make("auth"), _make("billing")]
    om = OwnerMap({"auth": "team-a"})
    resolved = resolve_results(results, om)
    missing = unowned(resolved)
    assert len(missing) == 1
    assert missing[0].result.service == "billing"


def test_unowned_empty_when_all_mapped():
    results = [_make("auth")]
    om = OwnerMap({"auth": "team-a"})
    resolved = resolve_results(results, om)
    assert unowned(resolved) == []
