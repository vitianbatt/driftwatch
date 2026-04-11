"""Tests for driftwatch/tracer.py."""
import pytest

from driftwatch.comparator import DriftResult
from driftwatch.tracer import FieldTrace, TraceReport, TracerError, build_trace


def _make(service: str, drifted=None) -> DriftResult:
    return DriftResult(service=service, drifted_fields=drifted or [])


# ---------------------------------------------------------------------------
# FieldTrace
# ---------------------------------------------------------------------------

class TestFieldTrace:
    def test_to_dict_keys(self):
        ft = FieldTrace(service="auth", field_name="replicas", occurrences=3)
        d = ft.to_dict()
        assert set(d.keys()) == {"service", "field_name", "occurrences"}

    def test_to_dict_values(self):
        ft = FieldTrace(service="auth", field_name="replicas", occurrences=3)
        d = ft.to_dict()
        assert d["service"] == "auth"
        assert d["field_name"] == "replicas"
        assert d["occurrences"] == 3


# ---------------------------------------------------------------------------
# TraceReport helpers
# ---------------------------------------------------------------------------

class TestTraceReport:
    def _report(self):
        traces = [
            FieldTrace("svc-a", "replicas", 3),
            FieldTrace("svc-a", "image", 1),
            FieldTrace("svc-b", "timeout", 2),
        ]
        return TraceReport(traces=traces)

    def test_persistent_default_threshold(self):
        report = self._report()
        p = report.persistent()
        assert len(p) == 2
        assert all(t.occurrences >= 2 for t in p)

    def test_transient_default_threshold(self):
        report = self._report()
        t = report.transient()
        assert len(t) == 1
        assert t[0].field_name == "image"

    def test_persistent_invalid_threshold_raises(self):
        report = self._report()
        with pytest.raises(TracerError):
            report.persistent(min_occurrences=0)

    def test_transient_invalid_threshold_raises(self):
        report = self._report()
        with pytest.raises(TracerError):
            report.transient(min_occurrences=0)

    def test_summary_no_traces(self):
        report = TraceReport(traces=[])
        assert "No drift traces" in report.summary()

    def test_summary_with_traces(self):
        report = self._report()
        s = report.summary()
        assert "3 field trace" in s
        assert "2 persistent" in s


# ---------------------------------------------------------------------------
# build_trace
# ---------------------------------------------------------------------------

class TestBuildTrace:
    def test_none_raises(self):
        with pytest.raises(TracerError):
            build_trace(None)

    def test_none_inner_list_raises(self):
        with pytest.raises(TracerError):
            build_trace([None])

    def test_empty_runs_returns_empty_report(self):
        report = build_trace([])
        assert report.traces == []

    def test_single_run_no_drift(self):
        report = build_trace([[_make("auth"), _make("billing")]])
        assert report.traces == []

    def test_single_run_with_drift(self):
        run = [_make("auth", ["replicas", "image"]), _make("billing", ["timeout"])]
        report = build_trace([run])
        assert len(report.traces) == 3
        assert all(t.occurrences == 1 for t in report.traces)

    def test_two_runs_accumulate_counts(self):
        run1 = [_make("auth", ["replicas"])]
        run2 = [_make("auth", ["replicas"])]
        report = build_trace([run1, run2])
        assert len(report.traces) == 1
        assert report.traces[0].occurrences == 2

    def test_field_only_in_one_run_is_transient(self):
        run1 = [_make("auth", ["replicas"])]
        run2 = [_make("auth", ["image"])]
        report = build_trace([run1, run2])
        assert len(report.persistent()) == 0
        assert len(report.transient()) == 2

    def test_traces_sorted_by_service_then_field(self):
        run = [_make("svc-b", ["z", "a"]), _make("svc-a", ["m"])]
        report = build_trace([run])
        keys = [(t.service, t.field_name) for t in report.traces]
        assert keys == sorted(keys)
