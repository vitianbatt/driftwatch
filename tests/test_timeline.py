"""Tests for driftwatch.timeline."""
import pytest

from driftwatch.timeline import (
    Timeline,
    TimelineError,
    TimelineEvent,
    build_timeline,
)


def _event(ts: str, drifted=None, resolved=None) -> dict:
    return {
        "timestamp": ts,
        "drifted_fields": drifted or [],
        "resolved_fields": resolved or [],
    }


# ---------------------------------------------------------------------------
# TimelineEvent
# ---------------------------------------------------------------------------

class TestTimelineEvent:
    def test_has_drift_false_when_empty(self):
        e = TimelineEvent(timestamp="2024-01-01T00:00:00", service="svc", drifted_fields=[])
        assert e.has_drift() is False

    def test_has_drift_true_when_fields_present(self):
        e = TimelineEvent(timestamp="2024-01-01T00:00:00", service="svc", drifted_fields=["env"])
        assert e.has_drift() is True

    def test_to_dict_contains_all_keys(self):
        e = TimelineEvent(
            timestamp="2024-01-02T12:00:00",
            service="auth",
            drifted_fields=["port", "env"],
            resolved_fields=["timeout"],
        )
        d = e.to_dict()
        assert set(d.keys()) == {"timestamp", "service", "drifted_fields", "resolved_fields", "has_drift"}

    def test_to_dict_sorts_fields(self):
        e = TimelineEvent(
            timestamp="t", service="s", drifted_fields=["z", "a"], resolved_fields=["m", "b"]
        )
        d = e.to_dict()
        assert d["drifted_fields"] == ["a", "z"]
        assert d["resolved_fields"] == ["b", "m"]


# ---------------------------------------------------------------------------
# build_timeline
# ---------------------------------------------------------------------------

class TestBuildTimeline:
    def test_none_events_raises(self):
        with pytest.raises(TimelineError):
            build_timeline("svc", None)

    def test_empty_service_raises(self):
        with pytest.raises(TimelineError):
            build_timeline("", [])

    def test_whitespace_service_raises(self):
        with pytest.raises(TimelineError):
            build_timeline("   ", [])

    def test_missing_timestamp_raises(self):
        with pytest.raises(TimelineError):
            build_timeline("svc", [{"drifted_fields": []}])

    def test_missing_drifted_fields_raises(self):
        with pytest.raises(TimelineError):
            build_timeline("svc", [{"timestamp": "2024-01-01T00:00:00"}])

    def test_empty_events_returns_empty_timeline(self):
        tl = build_timeline("svc", [])
        assert len(tl) == 0
        assert tl.service == "svc"

    def test_events_sorted_by_timestamp(self):
        raw = [
            _event("2024-01-03T00:00:00", drifted=["a"]),
            _event("2024-01-01T00:00:00", drifted=["b"]),
            _event("2024-01-02T00:00:00"),
        ]
        tl = build_timeline("svc", raw)
        timestamps = [e.timestamp for e in tl.events]
        assert timestamps == sorted(timestamps)

    def test_drift_events_filters_correctly(self):
        raw = [
            _event("2024-01-01T00:00:00"),
            _event("2024-01-02T00:00:00", drifted=["env"]),
            _event("2024-01-03T00:00:00", drifted=["port", "replicas"]),
        ]
        tl = build_timeline("svc", raw)
        assert len(tl.drift_events()) == 2

    def test_latest_returns_last_event(self):
        raw = [
            _event("2024-01-01T00:00:00"),
            _event("2024-01-05T00:00:00", drifted=["env"]),
        ]
        tl = build_timeline("svc", raw)
        assert tl.latest().timestamp == "2024-01-05T00:00:00"

    def test_latest_returns_none_when_empty(self):
        tl = build_timeline("svc", [])
        assert tl.latest() is None

    def test_summary_no_events(self):
        tl = build_timeline("auth", [])
        assert "no events" in tl.summary()

    def test_summary_counts_drift_events(self):
        raw = [
            _event("2024-01-01T00:00:00"),
            _event("2024-01-02T00:00:00", drifted=["env"]),
        ]
        tl = build_timeline("auth", raw)
        assert "1/2" in tl.summary()
        assert "auth" in tl.summary()
