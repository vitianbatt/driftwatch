"""Tests for driftwatch.batcher."""

from __future__ import annotations

import pytest

from driftwatch.batcher import Batch, BatchedReport, BatcherError, build_batches
from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


def _make(service: str, fields: list[str] | None = None) -> DriftResult:
    diffs = [FieldDiff(field=f, kind="missing", expected="x", actual=None) for f in (fields or [])]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

class TestBatch:
    def test_len_empty(self):
        b = Batch(index=0)
        assert len(b) == 0

    def test_len_with_results(self):
        b = Batch(index=0, results=[_make("svc-a"), _make("svc-b")])
        assert len(b) == 2

    def test_service_names(self):
        b = Batch(index=0, results=[_make("alpha"), _make("beta")])
        assert b.service_names() == ["alpha", "beta"]

    def test_has_any_drift_false_when_clean(self):
        b = Batch(index=0, results=[_make("svc-a")])
        assert b.has_any_drift() is False

    def test_has_any_drift_true_when_drifted(self):
        b = Batch(index=0, results=[_make("svc-a", ["port"])])
        assert b.has_any_drift() is True


# ---------------------------------------------------------------------------
# build_batches
# ---------------------------------------------------------------------------

class TestBuildBatches:
    def test_none_raises(self):
        with pytest.raises(BatcherError):
            build_batches(None)  # type: ignore[arg-type]

    def test_zero_batch_size_raises(self):
        with pytest.raises(BatcherError):
            build_batches([], batch_size=0)

    def test_negative_batch_size_raises(self):
        with pytest.raises(BatcherError):
            build_batches([], batch_size=-1)

    def test_empty_list_returns_empty_report(self):
        report = build_batches([])
        assert report.total_batches == 0
        assert report.total_results == 0

    def test_single_result_one_batch(self):
        report = build_batches([_make("svc-a")])
        assert report.total_batches == 1
        assert report.total_results == 1

    def test_exact_multiple_produces_correct_count(self):
        results = [_make(f"svc-{i}") for i in range(6)]
        report = build_batches(results, batch_size=3)
        assert report.total_batches == 2

    def test_remainder_creates_extra_batch(self):
        results = [_make(f"svc-{i}") for i in range(7)]
        report = build_batches(results, batch_size=3)
        assert report.total_batches == 3
        assert len(report.get_batch(2)) == 1  # type: ignore[arg-type]

    def test_get_batch_returns_none_for_missing_index(self):
        report = build_batches([_make("svc-a")], batch_size=5)
        assert report.get_batch(99) is None

    def test_batch_indices_are_sequential(self):
        results = [_make(f"svc-{i}") for i in range(5)]
        report = build_batches(results, batch_size=2)
        indices = [b.index for b in report.batches]
        assert indices == list(range(report.total_batches))

    def test_summary_no_batches(self):
        report = build_batches([])
        assert report.summary() == "No batches."

    def test_summary_with_drift(self):
        results = [_make("svc-a", ["port"]), _make("svc-b")]
        report = build_batches(results, batch_size=1)
        text = report.summary()
        assert "2 batch(es)" in text
        assert "1 batch(es) with drift" in text
