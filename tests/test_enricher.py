"""Tests for driftwatch.enricher."""

import pytest

from driftwatch.comparator import DriftResult
from driftwatch.enricher import EnrichedResult, EnricherError, enrich_results


def _make(service: str, missing=None, extra=None, changed=None) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=missing or [],
        extra_keys=extra or [],
        changed_keys=changed or [],
    )


META = {
    "auth-service": {
        "environment": "production",
        "region": "us-east-1",
        "owner": "platform-team",
        "tier": "critical",
    },
    "billing-service": {
        "environment": "staging",
        "region": "eu-west-1",
        "owner": "billing-team",
    },
}


class TestEnrichResults:
    def test_empty_results_returns_empty(self):
        assert enrich_results([], META) == []

    def test_none_results_raises(self):
        with pytest.raises(EnricherError, match="results"):
            enrich_results(None, META)  # type: ignore

    def test_none_meta_raises(self):
        with pytest.raises(EnricherError, match="meta"):
            enrich_results([_make("svc")], None)  # type: ignore

    def test_known_service_gets_metadata(self):
        results = enrich_results([_make("auth-service")], META)
        assert len(results) == 1
        r = results[0]
        assert r.environment == "production"
        assert r.region == "us-east-1"
        assert r.owner == "platform-team"

    def test_extra_meta_keys_become_tags(self):
        results = enrich_results([_make("auth-service")], META)
        assert results[0].tags == {"tier": "critical"}

    def test_unknown_service_gets_empty_strings(self):
        results = enrich_results([_make("unknown-svc")], META)
        r = results[0]
        assert r.environment == ""
        assert r.region == ""
        assert r.owner == ""
        assert r.tags == {}

    def test_multiple_results_enriched_independently(self):
        results = enrich_results(
            [_make("auth-service"), _make("billing-service")], META
        )
        assert results[0].environment == "production"
        assert results[1].environment == "staging"

    def test_drift_fields_preserved(self):
        dr = _make("auth-service", missing=["timeout"], changed=["replicas"])
        results = enrich_results([dr], META)
        assert results[0].result.missing_keys == ["timeout"]
        assert results[0].result.changed_keys == ["replicas"]


class TestEnrichedResultToDict:
    def test_to_dict_contains_all_keys(self):
        dr = _make("auth-service")
        er = EnrichedResult(
            result=dr,
            environment="production",
            region="us-east-1",
            owner="platform-team",
            tags={"tier": "critical"},
        )
        d = er.to_dict()
        assert d["service"] == "auth-service"
        assert d["environment"] == "production"
        assert d["region"] == "us-east-1"
        assert d["owner"] == "platform-team"
        assert d["tags"] == {"tier": "critical"}

    def test_to_dict_has_drift_flag(self):
        dr = _make("svc", missing=["key"])
        er = EnrichedResult(result=dr)
        assert er.to_dict()["has_drift"] is True
