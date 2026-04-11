"""Tests for driftwatch.pinpointer."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.pinpointer import (
    PinpointerError,
    PinnedField,
    PinpointReport,
    pinpoint,
)


def _diff(field, expected, actual):
    return FieldDiff(field=field, expected=expected, actual=actual)


def _make(service, diffs=None):
    return DriftResult(service=service, diffs=diffs or [])


# ---------------------------------------------------------------------------
# TestPinnedField
# ---------------------------------------------------------------------------

class TestPinnedField:
    def test_to_dict_keys(self):
        pf = PinnedField(service="svc", field_name="port", diff_type="missing", weight=3)
        d = pf.to_dict()
        assert set(d.keys()) == {"service", "field_name", "diff_type", "weight"}

    def test_to_dict_values(self):
        pf = PinnedField(service="svc", field_name="port", diff_type="changed", weight=2)
        assert pf.to_dict()["weight"] == 2


# ---------------------------------------------------------------------------
# TestPinpointReport
# ---------------------------------------------------------------------------

class TestPinpointReport:
    def test_summary_empty(self):
        report = PinpointReport()
        assert report.summary() == "No drift fields pinpointed."

    def test_summary_lists_fields(self):
        pf = PinnedField(service="auth", field_name="replicas", diff_type="missing", weight=3)
        report = PinpointReport(pinned=[pf])
        s = report.summary()
        assert "auth" in s
        assert "replicas" in s

    def test_top_returns_highest_weight_first(self):
        fields = [
            PinnedField("a", "x", "extra", 1),
            PinnedField("b", "y", "missing", 3),
            PinnedField("c", "z", "changed", 2),
        ]
        report = PinpointReport(pinned=fields)
        top = report.top(2)
        assert top[0].weight == 3
        assert top[1].weight == 2

    def test_top_respects_n(self):
        fields = [PinnedField("s", f"f{i}", "changed", i) for i in range(10)]
        report = PinpointReport(pinned=fields)
        assert len(report.top(3)) == 3


# ---------------------------------------------------------------------------
# TestPinpoint function
# ---------------------------------------------------------------------------

def test_none_raises():
    with pytest.raises(PinpointerError):
        pinpoint(None)


def test_empty_results_returns_empty_report():
    report = pinpoint([])
    assert report.pinned == []


def test_missing_field_weight_is_three():
    result = _make("svc", [_diff("port", 8080, None)])
    report = pinpoint([result])
    assert report.pinned[0].weight == 3
    assert report.pinned[0].diff_type == "missing"


def test_extra_field_weight_is_one():
    result = _make("svc", [_diff("debug", None, True)])
    report = pinpoint([result])
    assert report.pinned[0].weight == 1
    assert report.pinned[0].diff_type == "extra"


def test_changed_field_weight_is_two():
    result = _make("svc", [_diff("replicas", 3, 1)])
    report = pinpoint([result])
    assert report.pinned[0].weight == 2
    assert report.pinned[0].diff_type == "changed"


def test_multiple_services_all_pinned():
    results = [
        _make("auth", [_diff("port", 80, None), _diff("replicas", 3, 1)]),
        _make("gateway", [_diff("timeout", None, 30)]),
    ]
    report = pinpoint(results)
    assert len(report.pinned) == 3
    services = {p.service for p in report.pinned}
    assert services == {"auth", "gateway"}


def test_clean_result_produces_no_pins():
    report = pinpoint([_make("clean-svc", [])])
    assert report.pinned == []
