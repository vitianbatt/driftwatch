"""Tests for driftwatch.observer."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.observer import (
    FieldObservation,
    ObservationReport,
    ObserverError,
    observe,
)


def _diff(field: str, kind: str = "missing") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected=None, actual=None)


def _make(service: str, fields=()) -> DriftResult:
    diffs = [_diff(f) for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# FieldObservation.to_dict
# ---------------------------------------------------------------------------
class TestFieldObservation:
    def test_to_dict_keys(self):
        obs = FieldObservation(field_name="timeout", occurrences=3, services=["b", "a"])
        d = obs.to_dict()
        assert set(d.keys()) == {"field_name", "occurrences", "services"}

    def test_to_dict_services_sorted(self):
        obs = FieldObservation(field_name="x", occurrences=1, services=["z", "a", "m"])
        assert obs.to_dict()["services"] == ["a", "m", "z"]


# ---------------------------------------------------------------------------
# observe()
# ---------------------------------------------------------------------------
class TestObserve:
    def test_none_raises(self):
        with pytest.raises(ObserverError):
            observe(None)

    def test_empty_list_returns_empty_report(self):
        report = observe([])
        assert report.total_tracked() == 0
        assert report.field_names() == []

    def test_clean_results_not_tracked(self):
        results = [_make("svc-a"), _make("svc-b")]
        report = observe(results)
        assert report.total_tracked() == 0

    def test_single_drift_field_recorded(self):
        results = [_make("svc-a", ["timeout"])]
        report = observe(results)
        assert "timeout" in report.observations
        assert report.observations["timeout"].occurrences == 1

    def test_same_field_across_services_increments(self):
        results = [
            _make("svc-a", ["timeout"]),
            _make("svc-b", ["timeout"]),
        ]
        report = observe(results)
        obs = report.observations["timeout"]
        assert obs.occurrences == 2
        assert sorted(obs.services) == ["svc-a", "svc-b"]

    def test_same_service_same_field_counted_once_per_result(self):
        # Two separate DriftResult objects for the same service both count
        results = [
            _make("svc-a", ["timeout"]),
            _make("svc-a", ["timeout"]),
        ]
        report = observe(results)
        assert report.observations["timeout"].occurrences == 2
        # service only listed once
        assert report.observations["timeout"].services.count("svc-a") == 1

    def test_multiple_fields_tracked_independently(self):
        results = [_make("svc-a", ["timeout", "replicas"])]
        report = observe(results)
        assert "timeout" in report.observations
        assert "replicas" in report.observations

    def test_field_names_sorted(self):
        results = [_make("svc-a", ["z_field", "a_field", "m_field"])]
        report = observe(results)
        assert report.field_names() == ["a_field", "m_field", "z_field"]

    def test_top_returns_highest_occurrence_first(self):
        results = [
            _make("svc-a", ["timeout"]),
            _make("svc-b", ["timeout"]),
            _make("svc-c", ["timeout"]),
            _make("svc-a", ["replicas"]),
        ]
        report = observe(results)
        top = report.top(1)
        assert len(top) == 1
        assert top[0].field_name == "timeout"

    def test_top_n_limits_results(self):
        results = [_make("svc-a", ["f1", "f2", "f3", "f4", "f5", "f6"])]
        report = observe(results)
        assert len(report.top(3)) == 3

    def test_summary_no_observations(self):
        report = observe([])
        assert "No field" in report.summary()

    def test_summary_lists_fields(self):
        results = [_make("svc-a", ["timeout"])]
        report = observe(results)
        assert "timeout" in report.summary()
