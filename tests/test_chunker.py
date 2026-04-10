"""Tests for driftwatch/chunker.py"""

from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.chunker import (
    Chunk,
    ChunkedReport,
    ChunkerError,
    chunk_results,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, drifted: bool = False) -> DriftResult:
    fields = ["timeout"] if drifted else []
    return DriftResult(service=service, drifted=drifted, fields=fields)


# ---------------------------------------------------------------------------
# Chunk
# ---------------------------------------------------------------------------

class TestChunk:
    def test_len_empty(self):
        assert len(Chunk(index=0)) == 0

    def test_len_with_results(self):
        c = Chunk(index=0, results=[_make("svc-a"), _make("svc-b")])
        assert len(c) == 2

    def test_service_names(self):
        c = Chunk(index=1, results=[_make("alpha"), _make("beta")])
        assert c.service_names() == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# chunk_results – error cases
# ---------------------------------------------------------------------------

def test_none_results_raises():
    with pytest.raises(ChunkerError, match="None"):
        chunk_results(None, size=5)  # type: ignore[arg-type]


def test_zero_size_raises():
    with pytest.raises(ChunkerError, match=">= 1"):
        chunk_results([], size=0)


def test_negative_size_raises():
    with pytest.raises(ChunkerError, match=">= 1"):
        chunk_results([], size=-3)


# ---------------------------------------------------------------------------
# chunk_results – empty input
# ---------------------------------------------------------------------------

def test_empty_input_returns_one_empty_chunk():
    report = chunk_results([], size=3)
    assert report.total_chunks == 1
    assert report.total_results == 0
    assert len(report.chunks[0]) == 0


# ---------------------------------------------------------------------------
# chunk_results – normal cases
# ---------------------------------------------------------------------------

def test_single_result_single_chunk():
    report = chunk_results([_make("svc-a")], size=5)
    assert report.total_chunks == 1
    assert report.total_results == 1


def test_exact_multiple_produces_correct_chunk_count():
    results = [_make(f"svc-{i}") for i in range(6)]
    report = chunk_results(results, size=3)
    assert report.total_chunks == 2
    assert all(len(c) == 3 for c in report.chunks)


def test_remainder_chunk_smaller():
    results = [_make(f"svc-{i}") for i in range(7)]
    report = chunk_results(results, size=3)
    assert report.total_chunks == 3
    assert len(report.chunks[-1]) == 1


def test_chunk_indices_are_sequential():
    results = [_make(f"svc-{i}") for i in range(5)]
    report = chunk_results(results, size=2)
    for expected_idx, chunk in enumerate(report):
        assert chunk.index == expected_idx


def test_total_results_matches_input_length():
    results = [_make(f"svc-{i}", drifted=(i % 2 == 0)) for i in range(11)]
    report = chunk_results(results, size=4)
    assert report.total_results == 11


# ---------------------------------------------------------------------------
# ChunkedReport.summary
# ---------------------------------------------------------------------------

def test_summary_empty():
    report = chunk_results([], size=2)
    # empty input still yields one chunk
    assert "chunk" in report.summary()


def test_summary_non_empty():
    results = [_make(f"svc-{i}") for i in range(5)]
    report = chunk_results(results, size=2)
    summary = report.summary()
    assert "3" in summary   # 3 chunks
    assert "5" in summary   # 5 results
