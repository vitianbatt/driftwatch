"""Batch processing for drift results — groups results into fixed-size batches."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class BatcherError(Exception):
    """Raised when batching fails."""


@dataclass
class Batch:
    index: int
    results: List[DriftResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def has_any_drift(self) -> bool:
        return any(r.diffs for r in self.results)


@dataclass
class BatchedReport:
    batches: List[Batch] = field(default_factory=list)
    batch_size: int = 10

    @property
    def total_batches(self) -> int:
        return len(self.batches)

    @property
    def total_results(self) -> int:
        return sum(len(b) for b in self.batches)

    def get_batch(self, index: int) -> Optional[Batch]:
        for b in self.batches:
            if b.index == index:
                return b
        return None

    def summary(self) -> str:
        if not self.batches:
            return "No batches."
        drifted = sum(1 for b in self.batches if b.has_any_drift())
        return (
            f"{self.total_batches} batch(es), {self.total_results} result(s), "
            f"{drifted} batch(es) with drift."
        )


def build_batches(results: List[DriftResult], batch_size: int = 10) -> BatchedReport:
    """Split *results* into sequential batches of *batch_size*."""
    if results is None:
        raise BatcherError("results must not be None")
    if batch_size < 1:
        raise BatcherError("batch_size must be >= 1")

    batches: List[Batch] = []
    for i in range(0, len(results), batch_size):
        chunk = results[i : i + batch_size]
        batches.append(Batch(index=len(batches), results=chunk))

    return BatchedReport(batches=batches, batch_size=batch_size)
