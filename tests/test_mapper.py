"""Tests for driftwatch/mapper.py."""
from __future__ import annotations

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.mapper import (
    FieldMapping,
    MapperError,
    MappedResult,
    apply_mapping,
    build_mapping,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make(service: str, diff: dict | None = None, drift_fields: list | None = None) -> DriftResult:
    r = DriftResult(service=service)
    r.diff = diff or {}
    r.drift_fields = drift_fields or []
    return r


# ---------------------------------------------------------------------------
# FieldMapping
# ---------------------------------------------------------------------------

class TestFieldMapping:
    def test_valid_mapping_created(self):
        m = FieldMapping(source="replicas", destination="replica_count")
        assert m.source == "replicas"
        assert m.destination == "replica_count"

    def test_empty_source_raises(self):
        with pytest.raises(MapperError, match="source"):
            FieldMapping(source="", destination="replica_count")

    def test_whitespace_source_raises(self):
        with pytest.raises(MapperError, match="source"):
            FieldMapping(source="   ", destination="replica_count")

    def test_empty_destination_raises(self):
        with pytest.raises(MapperError, match="destination"):
            FieldMapping(source="replicas", destination="")


# ---------------------------------------------------------------------------
# build_mapping
# ---------------------------------------------------------------------------

def test_build_mapping_returns_field_mappings():
    raw = [{"source": "replicas", "destination": "replica_count"}]
    result = build_mapping(raw)
    assert len(result) == 1
    assert isinstance(result[0], FieldMapping)


def test_build_mapping_none_raises():
    with pytest.raises(MapperError):
        build_mapping(None)  # type: ignore[arg-type]


def test_build_mapping_missing_key_raises():
    with pytest.raises(MapperError, match="source.*destination"):
        build_mapping([{"source": "replicas"}])


# ---------------------------------------------------------------------------
# apply_mapping
# ---------------------------------------------------------------------------

def test_empty_results_returns_empty():
    assert apply_mapping([], []) == []


def test_none_results_raises():
    with pytest.raises(MapperError):
        apply_mapping(None, [])  # type: ignore[arg-type]


def test_none_mappings_raises():
    with pytest.raises(MapperError):
        apply_mapping([], None)  # type: ignore[arg-type]


def test_known_field_is_renamed():
    mappings = [FieldMapping(source="replicas", destination="replica_count")]
    result = _make("svc-a", diff={"replicas": 3}, drift_fields=["replicas"])
    mapped = apply_mapping([result], mappings)
    assert len(mapped) == 1
    assert "replica_count" in mapped[0].data
    assert "replicas" not in mapped[0].data
    assert "replica_count" in mapped[0].drift_fields


def test_unknown_field_passed_through():
    mappings = [FieldMapping(source="replicas", destination="replica_count")]
    result = _make("svc-b", diff={"image": "nginx:1.21"}, drift_fields=["image"])
    mapped = apply_mapping([result], mappings)
    assert mapped[0].data == {"image": "nginx:1.21"}
    assert mapped[0].drift_fields == ["image"]


def test_has_drift_false_when_no_drift_fields():
    result = _make("svc-c", diff={}, drift_fields=[])
    mapped = apply_mapping([result], [])
    assert not mapped[0].has_drift()


def test_has_drift_true_when_drift_fields_present():
    mappings = [FieldMapping(source="cpu", destination="cpu_limit")]
    result = _make("svc-d", diff={"cpu": "500m"}, drift_fields=["cpu"])
    mapped = apply_mapping([result], mappings)
    assert mapped[0].has_drift()


def test_to_dict_contains_expected_keys():
    result = _make("svc-e", diff={"mem": "256Mi"}, drift_fields=["mem"])
    mapped = apply_mapping([result], [])
    d = mapped[0].to_dict()
    assert set(d.keys()) == {"service", "data", "drift_fields"}
    assert d["service"] == "svc-e"


def test_multiple_mappings_applied():
    mappings = [
        FieldMapping(source="replicas", destination="replica_count"),
        FieldMapping(source="image", destination="container_image"),
    ]
    result = _make("svc-f", diff={"replicas": 2, "image": "app:v2"}, drift_fields=["replicas", "image"])
    mapped = apply_mapping([result], mappings)
    assert set(mapped[0].data.keys()) == {"replica_count", "container_image"}
    assert set(mapped[0].drift_fields) == {"replica_count", "container_image"}
