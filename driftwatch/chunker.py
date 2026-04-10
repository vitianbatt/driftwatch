"""chunker.py – splits a list of DriftResults into fixed-size chunks for
batch processing (e.g. paginated API calls, bulk notifications)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Iterator

from driftwatch.comparator import DriftResult


class ChunkerError(Exception):
    """Raised when chunking parameters are invalid."""


@dataclass
class Chunk:
    index: int  # zero-based chunk index
    results: List[DriftResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]


@dataclass
class ChunkedReport:
    chunks: List[Chunk] = field(default_factory=list)

    @property
    def total_chunks(self) -> int:
        return len(self.chunks)

    @property
    def total_results(self) -> int:
        return sum(len(c) for c in self.chunks)

    def __iter__(self) -> Iterator[Chunk]:
        return iter(self.chunks)

    def summary(self) -> str:
        if not self.chunks:
            return "No chunks produced."
        return (
            f"{self.total_chunks} chunk(s) covering "
            f"{self.total_results} result(s)."
        )


def chunk_results(results: List[DriftResult], size: int) -> ChunkedReport:
    """Partition *results* into chunks of at most *size* items each.

    Args:
        results: List of DriftResult objects to partition.
        size: Maximum number of results per chunk (must be >= 1).

    Returns:
        A ChunkedReport containing all chunks in order.

    Raises:
        ChunkerError: If *results* is None or *size* is less than 1.
    """
    if results is None:
        raise ChunkerError("results must not be None")
    if size < 1:
        raise ChunkerError(f"chunk size must be >= 1, got {size}")

    chunks: List[Chunk] = []
    for idx, start in enumerate(range(0, max(len(results), 1), size)):
        batch = results[start : start + size]
        if not batch and results:
            break
        chunks.append(Chunk(index=idx, results=batch))

    # Guarantee at least one (empty) chunk when input is empty
    if not chunks:
        chunks.append(Chunk(index=0, results=[]))

    return ChunkedReport(chunks=chunks)
