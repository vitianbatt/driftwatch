"""Tests for driftwatch.stenciler."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.stenciler import (
    StencilConfig,
    StenciledResult,
    StencilerError,
    apply_stencil,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _diff(field: str, kind: str = "changed") -> FieldDiff:
    return FieldDiff(field=field, kind=kind, expected="a", actual="b")


def _make(service: str, *fields: str) -> DriftResult:
    diffs = [_diff(f) for f in fields]
    return DriftResult(service=service, diffs=diffs)


# ---------------------------------------------------------------------------
# StencilConfig
# ---------------------------------------------------------------------------

class TestStencilConfig:
    def test_empty_allowed_fields_is_valid(self):
        cfg = StencilConfig()
        assert cfg.allowed_fields == []

    def test_none_allowed_fields_raises(self):
        with pytest.raises(StencilerError, match="allowed_fields must not be None"):
            StencilConfig(allowed_fields=None)

    def test_blank_entry_raises(self):
        with pytest.raises(StencilerError, match="blank entries"):
            StencilConfig(allowed_fields=["replicas", ""])

    def test_whitespace_entry_raises(self):
        with pytest.raises(StencilerError, match="blank entries"):
            StencilConfig(allowed_fields=["  "])

    def test_allows_returns_true_for_listed_field(self):
        cfg = StencilConfig(allowed_fields=["replicas", "image"])
        assert cfg.allows("replicas") is True

    def test_allows_returns_false_for_unlisted_field(self):
        cfg = StencilConfig(allowed_fields=["replicas"])
        assert cfg.allows("image") is False

    def test_empty_stencil_allows_everything(self):
        cfg = StencilConfig()
        assert cfg.allows("anything") is True


# ---------------------------------------------------------------------------
# apply_stencil
# ---------------------------------------------------------------------------

class TestApplyStencil:
    def test_none_results_raises(self):
        cfg = StencilConfig(allowed_fields=["replicas"])
        with pytest.raises(StencilerError, match="results must not be None"):
            apply_stencil(None, cfg)

    def test_none_config_raises(self):
        with pytest.raises(StencilerError, match="config must not be None"):
            apply_stencil([], None)

    def test_empty_results_returns_empty(self):
        cfg = StencilConfig(allowed_fields=["replicas"])
        assert apply_stencil([], cfg) == []

    def test_all_fields_retained_when_stencil_empty(self):
        r = _make("svc", "replicas", "image", "env")
        cfg = StencilConfig()
        out = apply_stencil([r], cfg)
        assert len(out) == 1
        assert len(out[0].diffs) == 3

    def test_unlisted_fields_removed(self):
        r = _make("svc", "replicas", "image", "env")
        cfg = StencilConfig(allowed_fields=["replicas"])
        out = apply_stencil([r], cfg)
        assert len(out[0].diffs) == 1
        assert out[0].diffs[0].field == "replicas"

    def test_original_diff_count_preserved(self):
        r = _make("svc", "replicas", "image", "env")
        cfg = StencilConfig(allowed_fields=["replicas"])
        out = apply_stencil([r], cfg)
        assert out[0].original_diff_count == 3

    def test_has_drift_false_when_all_stripped(self):
        r = _make("svc", "image", "env")
        cfg = StencilConfig(allowed_fields=["replicas"])
        out = apply_stencil([r], cfg)
        assert out[0].has_drift is False

    def test_has_drift_true_when_fields_retained(self):
        r = _make("svc", "replicas")
        cfg = StencilConfig(allowed_fields=["replicas"])
        out = apply_stencil([r], cfg)
        assert out[0].has_drift is True

    def test_to_dict_contains_expected_keys(self):
        r = _make("svc", "replicas")
        cfg = StencilConfig(allowed_fields=["replicas"])
        d = apply_stencil([r], cfg)[0].to_dict()
        assert set(d.keys()) == {
            "service",
            "original_diff_count",
            "retained_diff_count",
            "has_drift",
            "diffs",
        }

    def test_multiple_results_processed_independently(self):
        r1 = _make("alpha", "replicas", "image")
        r2 = _make("beta", "env")
        cfg = StencilConfig(allowed_fields=["replicas", "env"])
        out = apply_stencil([r1, r2], cfg)
        assert len(out) == 2
        assert len(out[0].diffs) == 1  # only replicas
        assert len(out[1].diffs) == 1  # only env
