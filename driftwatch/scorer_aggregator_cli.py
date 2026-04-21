"""CLI helpers for the scorer_aggregator module.

Provides utilities to load scored results from JSON, run aggregation,
and serialise the resulting AggregatedScoredReport back to JSON for
pipeline use.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from driftwatch.comparator import DriftResult
from driftwatch.differ import FieldDiff
from driftwatch.scorer import ScoredResult, ScoredReport
from driftwatch.scorer_aggregator import AggregatedScoredReport


def results_from_json(raw: list[dict[str, Any]]) -> list[ScoredResult]:
    """Deserialise a list of raw dicts into ScoredResult objects.

    Each dict is expected to have the keys produced by
    ``ScoredResult.to_dict()``::

        {
          "service": "auth-service",
          "score": 7,
          "diffs": [
            {"field": "replicas", "action": "changed",
             "expected": "3", "actual": "2"}
          ]
        }

    Args:
        raw: List of raw dicts to deserialise.

    Returns:
        List of :class:`ScoredResult` instances.

    Raises:
        ValueError: If a required key is missing from any entry.
    """
    results: list[ScoredResult] = []
    for entry in raw:
        try:
            service = entry["service"]
            score = int(entry["score"])
        except KeyError as exc:
            raise ValueError(f"Missing required key in scored result: {exc}") from exc

        diffs: list[FieldDiff] = []
        for d in entry.get("diffs", []):
            diffs.append(
                FieldDiff(
                    field=d["field"],
                    action=d["action"],
                    expected=d.get("expected"),
                    actual=d.get("actual"),
                )
            )

        drift_result = DriftResult(service=service, diffs=diffs)
        results.append(ScoredResult(result=drift_result, score=score))

    return results


def report_to_json(report: AggregatedScoredReport) -> dict[str, Any]:
    """Serialise an :class:`AggregatedScoredReport` to a plain dict.

    Args:
        report: The aggregated report to serialise.

    Returns:
        A JSON-serialisable dictionary.
    """
    return {
        "total": report.total,
        "drifted": report.drifted,
        "clean": report.clean,
        "drift_rate": report.drift_rate(),
        "top": [
            {
                "service": r.result.service,
                "score": r.score,
                "diffs": [
                    {
                        "field": d.field,
                        "action": d.action,
                        "expected": d.expected,
                        "actual": d.actual,
                    }
                    for d in r.result.diffs
                ],
            }
            for r in report.top()
        ],
    }


def run_scorer_aggregator(argv: list[str] | None = None) -> None:  # pragma: no cover
    """Entry point for the scorer-aggregator CLI subcommand.

    Reads a JSON array of scored results from *stdin*, aggregates them,
    and writes the JSON report to *stdout*.

    Usage::

        cat scored.json | python -m driftwatch.scorer_aggregator_cli
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Aggregate scored drift results and emit a summary."
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="Number of top-scored services to include in the report (default: 5).",
    )
    args = parser.parse_args(argv)

    raw = json.load(sys.stdin)
    scored = results_from_json(raw)
    report = AggregatedScoredReport(results=scored, top_n=args.top)
    print(json.dumps(report_to_json(report), indent=2))
