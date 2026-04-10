"""Tests for driftwatch/transformer.py."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.transformer import (
    FieldTransform,
    TransformReport,
    TransformerError,
    apply_transforms,
)


def _make(service: str = "svc", spec: dict | None = None, live: dict | None = None) -> DriftResult:
    return DriftResult(service=service, spec=spec or {}, live=live or {}, diffs=[])


# ---------------------------------------------------------------------------
# FieldTransform
# ---------------------------------------------------------------------------

class TestFieldTransform:
    def test_valid_transform_created(self):
        ft = FieldTransform(field="env", transform="lowercase")
        assert ft.field == "env"
        assert ft.transform == "lowercase"

    def test_empty_field_raises(self):
        with pytest.raises(TransformerError, match="non-empty"):
            FieldTransform(field="", transform="strip")

    def test_whitespace_field_raises(self):
        with pytest.raises(TransformerError, match="non-empty"):
            FieldTransform(field="   ", transform="strip")

    def test_unknown_transform_raises(self):
        with pytest.raises(TransformerError, match="unknown transform"):
            FieldTransform(field="env", transform="nonexistent")

    def test_lowercase_apply(self):
        ft = FieldTransform(field="env", transform="lowercase")
        assert ft.apply("PROD") == "prod"

    def test_uppercase_apply(self):
        ft = FieldTransform(field="env", transform="uppercase")
        assert ft.apply("prod") == "PROD"

    def test_strip_apply(self):
        ft = FieldTransform(field="env", transform="strip")
        assert ft.apply("  prod  ") == "prod"

    def test_to_int_apply(self):
        ft = FieldTransform(field="replicas", transform="to_int")
        assert ft.apply("3") == 3

    def test_to_str_apply(self):
        ft = FieldTransform(field="replicas", transform="to_str")
        assert ft.apply(42) == "42"


# ---------------------------------------------------------------------------
# TransformReport
# ---------------------------------------------------------------------------

class TestTransformReport:
    def test_summary_no_transforms(self):
        report = TransformReport(results=[], transforms_applied=0)
        assert "0 result" in report.summary()
        assert "0 field transform" in report.summary()

    def test_summary_with_transforms(self):
        report = TransformReport(results=[_make()], transforms_applied=2)
        assert "1 result" in report.summary()
        assert "2 field transform" in report.summary()


# ---------------------------------------------------------------------------
# apply_transforms
# ---------------------------------------------------------------------------

def test_none_results_raises():
    with pytest.raises(TransformerError, match="None"):
        apply_transforms(None, [])


def test_none_transforms_raises():
    with pytest.raises(TransformerError, match="None"):
        apply_transforms([], None)


def test_empty_results_returns_empty():
    report = apply_transforms([], [FieldTransform("env", "lowercase")])
    assert report.results == []
    assert report.transforms_applied == 0


def test_no_matching_field_leaves_spec_unchanged():
    r = _make(spec={"replicas": 2})
    report = apply_transforms([r], [FieldTransform("env", "lowercase")])
    assert report.results[0].spec == {"replicas": 2}
    assert report.transforms_applied == 0


def test_matching_field_is_transformed():
    r = _make(spec={"env": "PROD", "replicas": 2})
    report = apply_transforms([r], [FieldTransform("env", "lowercase")])
    assert report.results[0].spec["env"] == "prod"
    assert report.results[0].spec["replicas"] == 2
    assert report.transforms_applied == 1


def test_multiple_transforms_applied():
    r = _make(spec={"env": "  PROD  ", "region": "US-EAST"})
    transforms = [
        FieldTransform("env", "strip"),
        FieldTransform("region", "lowercase"),
    ]
    report = apply_transforms([r], transforms)
    assert report.results[0].spec["env"] == "  PROD  "  # strip not lowercase
    # strip is applied to env
    assert report.transforms_applied == 2


def test_original_result_not_mutated():
    r = _make(spec={"env": "PROD"})
    apply_transforms([r], [FieldTransform("env", "lowercase")])
    assert r.spec["env"] == "PROD"


def test_bad_to_int_raises_transformer_error():
    r = _make(spec={"replicas": "not-a-number"})
    with pytest.raises(TransformerError, match="to_int"):
        apply_transforms([r], [FieldTransform("replicas", "to_int")])


def test_multiple_results_all_transformed():
    results = [
        _make(service="a", spec={"env": "PROD"}),
        _make(service="b", spec={"env": "STAGING"}),
    ]
    report = apply_transforms(results, [FieldTransform("env", "lowercase")])
    assert report.results[0].spec["env"] == "prod"
    assert report.results[1].spec["env"] == "staging"
    assert report.transforms_applied == 2
