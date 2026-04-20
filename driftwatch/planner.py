"""Drift remediation planner: groups scored results into actionable work items."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from driftwatch.scorer import ScoredResult


class PlannerError(Exception):
    """Raised when planner inputs are invalid."""


@dataclass
class WorkItem:
    service: str
    score: float
    priority: str  # "critical" | "high" | "normal" | "low"
    fields: list[str]

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "score": self.score,
            "priority": self.priority,
            "fields": sorted(self.fields),
        }


@dataclass
class RemediationPlan:
    items: list[WorkItem] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.items)

    def by_priority(self, priority: str) -> list[WorkItem]:
        return [i for i in self.items if i.priority == priority]

    def summary(self) -> str:
        if not self.items:
            return "No remediation required."
        lines = [f"Remediation plan ({self.total} item(s)):"]
        for p in ("critical", "high", "normal", "low"):
            group = self.by_priority(p)
            if group:
                lines.append(f"  [{p.upper()}] {len(group)} service(s)")
                for item in group:
                    lines.append(f"    - {item.service} (score={item.score:.1f})")
        return "\n".join(lines)


def _priority_for_score(score: float) -> str:
    if score >= 20:
        return "critical"
    if score >= 10:
        return "high"
    if score >= 4:
        return "normal"
    return "low"


def build_plan(results: Iterable[ScoredResult]) -> RemediationPlan:
    """Convert scored results into a sorted remediation plan."""
    if results is None:
        raise PlannerError("results must not be None")
    items: list[WorkItem] = []
    for r in results:
        if r.score <= 0:
            continue
        items.append(
            WorkItem(
                service=r.service,
                score=r.score,
                priority=_priority_for_score(r.score),
                fields=[d.field for d in r.diffs],
            )
        )
    items.sort(key=lambda i: i.score, reverse=True)
    return RemediationPlan(items=items)
