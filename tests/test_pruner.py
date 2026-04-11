"""Tests for driftwatch.pruner."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.pruner import PruneConfig, PrunedReport, PrunerError, prune


def _diff(field_name: str = "replicas") -> FieldDiff:
    return FieldDiff(field=field_name, expected="2", actual="3", kind="changed")


def _make(service: str, diffs=None, timestamp=None) -> DriftResult:
    r = DriftResult(service=service, diffs=diffs or [])
    if timestamp is not None:
        r.timestamp = timestamp
    return r


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# PruneConfig validation
# ---------------------------------------------------------------------------

class TestPruneConfig:
    def test_valid_config_no_args(self):
        cfg = PruneConfig()
        assert cfg.max_age_seconds is None
        assert cfg.excluded_fields == []
        assert cfg.drop_clean is False

    def test_valid_max_age(self):
        cfg = PruneConfig(max_age_seconds=300)
        assert cfg.max_age_seconds == 300

    def test_zero_max_age_raises(self):
        with pytest.raises(PrunerError, match="positive"):
            PruneConfig(max_age_seconds=0)

    def test_negative_max_age_raises(self):
        with pytest.raises(PrunerError, match="positive"):
            PruneConfig(max_age_seconds=-1)

    def test_invalid_excluded_fields_type_raises(self):
        with pytest.raises(PrunerError, match="list"):
            PruneConfig(excluded_fields="replicas")  # type: ignore


# ---------------------------------------------------------------------------
# prune() – basic guards
# ---------------------------------------------------------------------------

def test_none_results_raises():
    with pytest.raises(PrunerError, match="None"):
        prune(None, PruneConfig())  # type: ignore


def test_none_config_raises():
    with pytest.raises(PrunerError, match="None"):
        prune([], None)  # type: ignore


def test_empty_list_returns_empty_report():
    report = prune([], PruneConfig(), now=NOW)
    assert report.kept == []
    assert report.removed_count == 0
    assert report.total() == 0


# ---------------------------------------------------------------------------
# drop_clean
# ---------------------------------------------------------------------------

def test_drop_clean_removes_no_drift_results():
    results = [_make("svc-a"), _make("svc-b", diffs=[_diff()])]
    report = prune(results, PruneConfig(drop_clean=True), now=NOW)
    assert len(report.kept) == 1
    assert report.kept[0].service == "svc-b"
    assert report.removed_count == 1


def test_drop_clean_false_keeps_all():
    results = [_make("svc-a"), _make("svc-b", diffs=[_diff()])]
    report = prune(results, PruneConfig(drop_clean=False), now=NOW)
    assert len(report.kept) == 2
    assert report.removed_count == 0


# ---------------------------------------------------------------------------
# max_age_seconds
# ---------------------------------------------------------------------------

def test_old_result_is_pruned():
    old_ts = NOW - timedelta(seconds=400)
    results = [_make("svc-a", timestamp=old_ts)]
    report = prune(results, PruneConfig(max_age_seconds=300), now=NOW)
    assert report.kept == []
    assert report.removed_count == 1


def test_fresh_result_is_kept():
    fresh_ts = NOW - timedelta(seconds=100)
    results = [_make("svc-a", timestamp=fresh_ts)]
    report = prune(results, PruneConfig(max_age_seconds=300), now=NOW)
    assert len(report.kept) == 1
    assert report.removed_count == 0


def test_result_without_timestamp_kept_when_age_set():
    results = [_make("svc-a")]
    report = prune(results, PruneConfig(max_age_seconds=300), now=NOW)
    assert len(report.kept) == 1


def test_string_iso_timestamp_parsed():
    old_str = (NOW - timedelta(seconds=500)).isoformat()
    results = [_make("svc-a", timestamp=old_str)]
    report = prune(results, PruneConfig(max_age_seconds=300), now=NOW)
    assert report.removed_count == 1


# ---------------------------------------------------------------------------
# excluded_fields
# ---------------------------------------------------------------------------

def test_excluded_field_stripped_from_diffs():
    diffs = [_diff("replicas"), _diff("image")]
    results = [_make("svc-a", diffs=diffs)]
    report = prune(results, PruneConfig(excluded_fields=["replicas"]), now=NOW)
    assert len(report.kept) == 1
    remaining_fields = [d.field for d in report.kept[0].diffs]
    assert "replicas" not in remaining_fields
    assert "image" in remaining_fields


def test_no_excluded_fields_leaves_diffs_intact():
    diffs = [_diff("replicas"), _diff("image")]
    results = [_make("svc-a", diffs=diffs)]
    report = prune(results, PruneConfig(), now=NOW)
    assert len(report.kept[0].diffs) == 2


# ---------------------------------------------------------------------------
# PrunedReport helpers
# ---------------------------------------------------------------------------

def test_summary_no_results():
    report = PrunedReport(kept=[], removed_count=0)
    assert report.summary() == "No results to prune."


def test_summary_with_pruned():
    report = PrunedReport(kept=[_make("svc-a")], removed_count=2)
    assert "Kept 1" in report.summary()
    assert "pruned 2" in report.summary()
    assert "3 total" in report.summary()
