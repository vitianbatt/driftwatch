"""ranker.py – rank DriftResult objects by severity and drift field count."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from driftwatch.comparator import DriftResult
from driftwatch.filter import Severity, _result_severity


class RankerError(Exception):
    """Raised when ranking fails."""


_SEVERITY_WEIGHT: dict[Severity, int] = {
    Severity.HIGH: 100,
    Severity.MEDIUM: 10,
    Severity.LOW: 1,
}


@dataclass
class RankedResult:
    result: DriftResult
    rank: int
    score: int
    severity: Severity

    def to_dict(self) -> dict:
        return {
            "service": self.result.service,
            "rank": self.rank,
            "score": self.score,
            "severity": self.severity.value,
            "drift_fields": len(self.result.diffs),
        }


@dataclass
class RankedReport:
    ranked: List[RankedResult] = field(default_factory=list)

    def top(self, n: int) -> List[RankedResult]:
        if n < 0:
            raise RankerError("n must be non-negative")
        return self.ranked[:n]

    def summary(self) -> str:
        if not self.ranked:
            return "No results to rank."
        lines = [f"{'Rank':<6} {'Service':<30} {'Score':<8} {'Severity':<10} {'Drifted Fields'}"]
        for rr in self.ranked:
            lines.append(
                f"{rr.rank:<6} {rr.result.service:<30} {rr.score:<8} {rr.severity.value:<10} {len(rr.result.diffs)}"
            )
        return "\n".join(lines)


def _score(result: DriftResult) -> int:
    sev = _result_severity(result)
    weight = _SEVERITY_WEIGHT.get(sev, 1)
    return weight + len(result.diffs)


def rank_results(results: Optional[List[DriftResult]]) -> RankedReport:
    """Rank results from most severe / most drifted to least."""
    if results is None:
        raise RankerError("results must not be None")

    scored = sorted(
        results,
        key=lambda r: (_score(r), len(r.diffs)),
        reverse=True,
    )

    ranked: List[RankedResult] = []
    for idx, result in enumerate(scored, start=1):
        sev = _result_severity(result)
        ranked.append(
            RankedResult(
                result=result,
                rank=idx,
                score=_score(result),
                severity=sev,
            )
        )

    return RankedReport(ranked=ranked)
