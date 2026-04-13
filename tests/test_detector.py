"""Tests for driftwatch.detector."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.detector import (
    DetectedChange,
    DetectionReport,
    DetectorError,
    detect_changes,
)


def _make(service: str, drifted=None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=drifted or [])


# ---------------------------------------------------------------------------
# DetectedChange
# ---------------------------------------------------------------------------

class TestDetectedChange:
    def test_has_change_false_when_empty(self):
        c = DetectedChange(service="svc")
        assert c.has_change() is False

    def test_has_change_true_when_appeared(self):
        c = DetectedChange(service="svc", appeared=["cpu"])
        assert c.has_change() is True

    def test_has_change_true_when_disappeared(self):
        c = DetectedChange(service="svc", disappeared=["mem"])
        assert c.has_change() is True

    def test_to_dict_keys(self):
        c = DetectedChange(service="svc", appeared=["cpu"], disappeared=["mem"])
        d = c.to_dict()
        assert set(d.keys()) == {"service", "appeared", "disappeared"}

    def test_to_dict_values_sorted(self):
        c = DetectedChange(service="svc", appeared=["z", "a"])
        assert c.to_dict()["appeared"] == ["a", "z"]


# ---------------------------------------------------------------------------
# DetectionReport
# ---------------------------------------------------------------------------

class TestDetectionReport:
    def test_any_changes_false_when_empty(self):
        r = DetectionReport(changes=[])
        assert r.any_changes() is False

    def test_any_changes_true_when_change_present(self):
        c = DetectedChange(service="svc", appeared=["cpu"])
        r = DetectionReport(changes=[c])
        assert r.any_changes() is True

    def test_summary_no_changes(self):
        r = DetectionReport(changes=[DetectedChange(service="svc")])
        assert r.summary() == "No drift changes detected."

    def test_summary_lists_appeared(self):
        c = DetectedChange(service="auth", appeared=["cpu"])
        r = DetectionReport(changes=[c])
        assert "appeared" in r.summary()
        assert "auth" in r.summary()


# ---------------------------------------------------------------------------
# detect_changes
# ---------------------------------------------------------------------------

def test_none_previous_raises():
    with pytest.raises(DetectorError):
        detect_changes(None, [])


def test_none_current_raises():
    with pytest.raises(DetectorError):
        detect_changes([], None)


def test_empty_lists_returns_empty_report():
    report = detect_changes([], [])
    assert report.changes == []
    assert report.any_changes() is False


def test_new_field_detected_as_appeared():
    prev = [_make("auth", [])]
    curr = [_make("auth", ["cpu"])]
    report = detect_changes(prev, curr)
    change = report.changes[0]
    assert change.service == "auth"
    assert "cpu" in change.appeared
    assert change.disappeared == []


def test_removed_field_detected_as_disappeared():
    prev = [_make("auth", ["cpu"])]
    curr = [_make("auth", [])]
    report = detect_changes(prev, curr)
    change = report.changes[0]
    assert "cpu" in change.disappeared
    assert change.appeared == []


def test_service_only_in_current_has_all_appeared():
    prev = []
    curr = [_make("new-svc", ["timeout"])]
    report = detect_changes(prev, curr)
    assert report.changes[0].appeared == ["timeout"]


def test_service_only_in_previous_has_all_disappeared():
    prev = [_make("old-svc", ["mem"])]
    curr = []
    report = detect_changes(prev, curr)
    assert report.changes[0].disappeared == ["mem"]


def test_unchanged_service_produces_no_change():
    prev = [_make("stable", ["cpu"])]
    curr = [_make("stable", ["cpu"])]
    report = detect_changes(prev, curr)
    assert not report.changes[0].has_change()


def test_services_sorted_in_report():
    prev = [_make("zebra"), _make("alpha")]
    curr = [_make("zebra"), _make("alpha")]
    report = detect_changes(prev, curr)
    names = [c.service for c in report.changes]
    assert names == sorted(names)
