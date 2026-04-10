"""transformer.py — apply field-level transformations to DriftResults before comparison."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List

from driftwatch.comparator import DriftResult


class TransformerError(Exception):
    """Raised when a transformation cannot be applied."""


# Built-in transform functions keyed by name.
_BUILTIN: Dict[str, Callable[[Any], Any]] = {
    "lowercase": lambda v: v.lower() if isinstance(v, str) else v,
    "uppercase": lambda v: v.upper() if isinstance(v, str) else v,
    "strip": lambda v: v.strip() if isinstance(v, str) else v,
    "to_int": lambda v: int(v),
    "to_str": lambda v: str(v),
}


@dataclass
class FieldTransform:
    """Maps a field name to a named built-in transform."""

    field: str
    transform: str

    def __post_init__(self) -> None:
        if not self.field or not self.field.strip():
            raise TransformerError("field must be a non-empty string")
        if self.transform not in _BUILTIN:
            raise TransformerError(
                f"unknown transform '{self.transform}'; "
                f"valid options: {sorted(_BUILTIN)}"
            )

    def apply(self, value: Any) -> Any:
        return _BUILTIN[self.transform](value)


@dataclass
class TransformReport:
    """Holds transformed copies of DriftResults."""

    results: List[DriftResult] = field(default_factory=list)
    transforms_applied: int = 0

    def summary(self) -> str:
        n = len(self.results)
        return (
            f"{n} result(s) processed, "
            f"{self.transforms_applied} field transform(s) applied"
        )


def apply_transforms(
    results: List[DriftResult],
    transforms: List[FieldTransform],
) -> TransformReport:
    """Return a TransformReport with spec values rewritten by *transforms*.

    Only spec-side values are mutated so that comparisons remain meaningful.
    The original result objects are not modified.
    """
    if results is None:
        raise TransformerError("results must not be None")
    if transforms is None:
        raise TransformerError("transforms must not be None")

    transform_map: Dict[str, FieldTransform] = {t.field: t for t in transforms}
    applied = 0
    out: List[DriftResult] = []

    for r in results:
        new_spec = dict(r.spec)
        for fname, ft in transform_map.items():
            if fname in new_spec:
                try:
                    new_spec[fname] = ft.apply(new_spec[fname])
                    applied += 1
                except (ValueError, TypeError) as exc:
                    raise TransformerError(
                        f"transform '{ft.transform}' failed on field "
                        f"'{fname}' for service '{r.service}': {exc}"
                    ) from exc
        out.append(
            DriftResult(
                service=r.service,
                spec=new_spec,
                live=r.live,
                diffs=r.diffs,
            )
        )

    return TransformReport(results=out, transforms_applied=applied)
