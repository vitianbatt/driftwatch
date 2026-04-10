"""sampler.py — randomly sample a subset of DriftResults for spot-checking."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult


class SamplerError(Exception):
    """Raised when sampling parameters are invalid."""


@dataclass
class SampleReport:
    sampled: List[DriftResult]
    total_input: int
    seed: Optional[int]

    def __len__(self) -> int:
        return len(self.sampled)

    def service_names(self) -> List[str]:
        return [r.service for r in self.sampled]

    def summary(self) -> str:
        if not self.sampled:
            return "No results sampled."
        drift_count = sum(1 for r in self.sampled if r.has_drift())
        return (
            f"Sampled {len(self.sampled)} of {self.total_input} results; "
            f"{drift_count} with drift."
        )


def sample_results(
    results: List[DriftResult],
    n: int,
    seed: Optional[int] = None,
) -> SampleReport:
    """Return a SampleReport containing up to *n* randomly chosen results.

    Args:
        results: Full list of DriftResult objects to sample from.
        n:       Number of results to select. Must be >= 1.
        seed:    Optional RNG seed for reproducibility.

    Raises:
        SamplerError: If *results* is None, or *n* is less than 1.
    """
    if results is None:
        raise SamplerError("results must not be None")
    if n < 1:
        raise SamplerError(f"n must be >= 1, got {n}")

    rng = random.Random(seed)
    chosen = rng.sample(results, min(n, len(results)))
    return SampleReport(sampled=chosen, total_input=len(results), seed=seed)
