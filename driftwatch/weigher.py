"""Field-weight map for the drift scorer."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class WeigherError(Exception):
    """Raised when a WeightMap is invalid."""


@dataclass
class WeightMap:
    weights: dict[str, float]
    default: float = 1.0

    def __post_init__(self) -> None:
        for key, val in self.weights.items():
            if not key or not key.strip():
                raise WeigherError("Weight key must not be empty or whitespace.")
            if val < 0:
                raise WeigherError(
                    f"Weight for '{key}' must be non-negative, got {val}."
                )
        if self.default < 0:
            raise WeigherError(
                f"Default weight must be non-negative, got {self.default}."
            )

    def get(self, field_name: str) -> float:
        """Return the weight for *field_name*, falling back to the default."""
        return self.weights.get(field_name, self.default)


@dataclass
class WeighedResult:
    service: str
    field: str
    weight: float
    kind: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "field": self.field,
            "weight": self.weight,
            "kind": self.kind,
        }


def weigh_diffs(
    service: str,
    diffs: list[Any],
    weight_map: WeightMap,
) -> list[WeighedResult]:
    """Return a WeighedResult for every FieldDiff in *diffs*."""
    return [
        WeighedResult(
            service=service,
            field=d.field,
            weight=weight_map.get(d.field),
            kind=d.kind,
        )
        for d in diffs
    ]


def total_weight(results: list[WeighedResult]) -> float:
    """Sum the weights of all WeighedResult entries."""
    return sum(r.weight for r in results)
