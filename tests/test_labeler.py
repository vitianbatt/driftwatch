"""Tests for driftwatch.labeler."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.labeler import (
    LabelerError,
    LabeledResult,
    filter_by_label,
    label_results,
)


def _make(service: str, drifted: list[str] | None = None) -> DriftResult:
    fields = drifted or []
    return DriftResult(service=service, has_drift=bool(fields), drifted_fields=fields)


# ---------------------------------------------------------------------------
# TestLabeledResult
# ---------------------------------------------------------------------------

class TestLabeledResult:
    def test_has_label_true(self):
        lr = LabeledResult(result=_make("svc"), labels={"env": "prod"})
        assert lr.has_label("env") is True

    def test_has_label_false(self):
        lr = LabeledResult(result=_make("svc"), labels={})
        assert lr.has_label("env") is False

    def test_get_label_returns_value(self):
        lr = LabeledResult(result=_make("svc"), labels={"team": "platform"})
        assert lr.get_label("team") == "platform"

    def test_get_label_returns_default(self):
        lr = LabeledResult(result=_make("svc"), labels={})
        assert lr.get_label("team", "unknown") == "unknown"

    def test_to_dict_structure(self):
        r = _make("auth", ["port"])
        lr = LabeledResult(result=r, labels={"env": "staging"})
        d = lr.to_dict()
        assert d["service"] == "auth"
        assert d["has_drift"] is True
        assert "port" in d["drifted_fields"]
        assert d["labels"] == {"env": "staging"}


# ---------------------------------------------------------------------------
# TestLabelResults
# ---------------------------------------------------------------------------

class TestLabelResults:
    def test_empty_results_returns_empty(self):
        assert label_results([], {}) == []

    def test_none_results_raises(self):
        with pytest.raises(LabelerError, match="results"):
            label_results(None, {})

    def test_none_label_map_raises(self):
        with pytest.raises(LabelerError, match="label_map"):
            label_results([], None)

    def test_labels_attached_correctly(self):
        results = [_make("auth"), _make("billing")]
        lmap = {"auth": {"env": "prod"}, "billing": {"env": "dev"}}
        labeled = label_results(results, lmap)
        assert labeled[0].get_label("env") == "prod"
        assert labeled[1].get_label("env") == "dev"

    def test_missing_service_gets_empty_labels(self):
        results = [_make("unknown-svc")]
        labeled = label_results(results, {})
        assert labeled[0].labels == {}

    def test_label_map_not_mutated(self):
        results = [_make("auth")]
        lmap = {"auth": {"env": "prod"}}
        original = dict(lmap["auth"])
        label_results(results, lmap)
        assert lmap["auth"] == original


# ---------------------------------------------------------------------------
# TestFilterByLabel
# ---------------------------------------------------------------------------

class TestFilterByLabel:
    def _labeled(self):
        r1 = LabeledResult(result=_make("auth"), labels={"env": "prod"})
        r2 = LabeledResult(result=_make("billing"), labels={"env": "dev"})
        r3 = LabeledResult(result=_make("gateway"), labels={"env": "prod"})
        return [r1, r2, r3]

    def test_filter_returns_matching(self):
        results = filter_by_label(self._labeled(), "env", "prod")
        services = [lr.result.service for lr in results]
        assert services == ["auth", "gateway"]

    def test_filter_no_match_returns_empty(self):
        results = filter_by_label(self._labeled(), "env", "staging")
        assert results == []

    def test_none_labeled_raises(self):
        with pytest.raises(LabelerError):
            filter_by_label(None, "env", "prod")

    def test_missing_key_excluded(self):
        lr = LabeledResult(result=_make("svc"), labels={})
        results = filter_by_label([lr], "env", "prod")
        assert results == []
