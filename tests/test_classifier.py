"""Tests for driftwatch.classifier."""

from __future__ import annotations

import pytest

from driftwatch.classifier import (
    ClassificationRule,
    ClassifiedResult,
    ClassifierError,
    classify_results,
)
from driftwatch.comparator import DriftResult


def _make(service: str, missing=(), extra=()) -> DriftResult:
    return DriftResult(service=service, missing_keys=list(missing), extra_keys=list(extra))


class TestClassificationRule:
    def test_valid_rule_created(self):
        rule = ClassificationRule(category="network", pattern=r"^port")
        assert rule.category == "network"

    def test_empty_category_raises(self):
        with pytest.raises(ClassifierError, match="category"):
            ClassificationRule(category="", pattern=r".*")

    def test_whitespace_category_raises(self):
        with pytest.raises(ClassifierError, match="category"):
            ClassificationRule(category="   ", pattern=r".*")

    def test_empty_pattern_raises(self):
        with pytest.raises(ClassifierError, match="pattern"):
            ClassificationRule(category="net", pattern="")

    def test_invalid_regex_raises(self):
        with pytest.raises(ClassifierError, match="invalid regex"):
            ClassificationRule(category="net", pattern="[unclosed")


class TestClassifiedResult:
    def test_has_category_true(self):
        r = ClassifiedResult(service="svc", categories=["network"])
        assert r.has_category("network") is True

    def test_has_category_false(self):
        r = ClassifiedResult(service="svc", categories=[])
        assert r.has_category("network") is False

    def test_to_dict_keys(self):
        r = ClassifiedResult(service="svc", categories=["auth"], unclassified_fields=["foo"])
        d = r.to_dict()
        assert set(d.keys()) == {"service", "categories", "unclassified_fields"}

    def test_to_dict_categories_sorted(self):
        r = ClassifiedResult(service="svc", categories=["z", "a"])
        assert r.to_dict()["categories"] == ["a", "z"]


class TestClassifyResults:
    def test_none_results_raises(self):
        with pytest.raises(ClassifierError):
            classify_results(None, [])

    def test_none_rules_raises(self):
        with pytest.raises(ClassifierError):
            classify_results([], None)

    def test_empty_results_returns_empty(self):
        assert classify_results([], []) == []

    def test_no_drift_produces_empty_categories(self):
        result = _make("svc")
        rules = [ClassificationRule(category="net", pattern=r"^port")]
        classified = classify_results([result], rules)
        assert classified[0].categories == []
        assert classified[0].unclassified_fields == []

    def test_matching_field_assigned_category(self):
        result = _make("svc", missing=["port_http"])
        rules = [ClassificationRule(category="network", pattern=r"^port")]
        classified = classify_results([result], rules)
        assert "network" in classified[0].categories

    def test_unmatched_field_is_unclassified(self):
        result = _make("svc", missing=["unknown_field"])
        rules = [ClassificationRule(category="network", pattern=r"^port")]
        classified = classify_results([result], rules)
        assert "unknown_field" in classified[0].unclassified_fields

    def test_multiple_fields_multiple_categories(self):
        result = _make("svc", missing=["port_http", "auth_token"])
        rules = [
            ClassificationRule(category="network", pattern=r"^port"),
            ClassificationRule(category="auth", pattern=r"^auth"),
        ]
        classified = classify_results([result], rules)
        assert "network" in classified[0].categories
        assert "auth" in classified[0].categories

    def test_category_deduped_across_fields(self):
        result = _make("svc", missing=["port_http", "port_https"])
        rules = [ClassificationRule(category="network", pattern=r"^port")]
        classified = classify_results([result], rules)
        assert classified[0].categories.count("network") == 1

    def test_multiple_results_classified_independently(self):
        r1 = _make("svc-a", missing=["port_http"])
        r2 = _make("svc-b", missing=["auth_key"])
        rules = [
            ClassificationRule(category="network", pattern=r"^port"),
            ClassificationRule(category="auth", pattern=r"^auth"),
        ]
        classified = classify_results([r1, r2], rules)
        assert classified[0].service == "svc-a"
        assert classified[1].service == "svc-b"
        assert "network" in classified[0].categories
        assert "auth" in classified[1].categories
