"""cutter.py — trims DriftResult lists to only fields matching a given prefix or suffix pattern."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff


class CutterError(Exception):
    """Raised when cutting configuration is invalid."""


@dataclass
class CutConfig:
    prefix: Optional[str] = None
    suffix: Optional[str] = None

    def __post_init__(self) -> None:
        if self.prefix is not None and not self.prefix.strip():
            raise CutterError("prefix must not be blank")
        if self.suffix is not None and not self.suffix.strip():
            raise CutterError("suffix must not be blank")
        if self.prefix is None and self.suffix is None:
            raise CutterError("at least one of prefix or suffix must be provided")


@dataclass
class CutResult:
    service: str
    diffs: List[FieldDiff] = field(default_factory=list)

    def has_drift(self) -> bool:
        return len(self.diffs) > 0

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "has_drift": self.has_drift(),
            "diffs": [{"field": d.field, "expected": d.expected, "actual": d.actual} for d in self.diffs],
        }


@dataclass
class CutReport:
    results: List[CutResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.results)

    def service_names(self) -> List[str]:
        return [r.service for r in self.results]

    def total_with_drift(self) -> int:
        return sum(1 for r in self.results if r.has_drift())


def _field_matches(name: str, config: CutConfig) -> bool:
    prefix_ok = config.prefix is None or name.startswith(config.prefix)
    suffix_ok = config.suffix is None or name.endswith(config.suffix)
    return prefix_ok and suffix_ok


def cut_results(results: List[DriftResult], config: CutConfig) -> CutReport:
    if results is None:
        raise CutterError("results must not be None")
    if config is None:
        raise CutterError("config must not be None")

    cut: List[CutResult] = []
    for r in results:
        kept = [d for d in (r.diffs if r.diffs else []) if _field_matches(d.field, config)]
        cut.append(CutResult(service=r.service, diffs=kept))
    return CutReport(results=cut)
