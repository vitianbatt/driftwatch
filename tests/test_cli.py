"""Tests for the driftwatch CLI entry point."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from driftwatch.cli import run, build_parser
from driftwatch.comparator import DriftResult
from driftwatch.loader import SpecLoadError
from driftwatch.watcher import WatchError


FIXTURES = Path(__file__).parent / "fixtures"


def _make_result(service: str, missing=(), extra=(), changed=()) -> DriftResult:
    return DriftResult(
        service=service,
        missing_keys=list(missing),
        extra_keys=list(extra),
        changed_values={k: (a, b) for k, a, b in changed},
    )


class TestBuildParser:
    def test_check_command_requires_targets(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["check", "spec.yaml"])

    def test_check_command_parses_correctly(self):
        parser = build_parser()
        args = parser.parse_args(["check", "spec.yaml", "--targets", "targets.yaml"])
        assert args.command == "check"
        assert args.spec == "spec.yaml"
        assert args.targets == "targets.yaml"
        assert args.output_format == "text"
        assert args.fail_on_drift is False

    def test_format_json_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["check", "spec.yaml", "--targets", "t.yaml", "--format", "json"]
        )
        assert args.output_format == "json"

    def test_invalid_format_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["check", "spec.yaml", "--targets", "t.yaml", "--format", "xml"]
            )


class TestRunCommand:
    def _patch_all(self, specs, live_configs, results):
        patches = [
            patch("driftwatch.cli.load_spec", return_value=specs),
            patch("driftwatch.cli.load_specs_from_dir", return_value={"svc": specs}),
            patch("driftwatch.cli.watch_all", return_value=live_configs),
            patch("driftwatch.cli.compare", side_effect=lambda svc, s, l: results[svc]),
        ]
        return patches

    def test_returns_zero_on_no_drift(self, tmp_path):
        spec_file = tmp_path / "svc.yaml"
        spec_file.write_text("key: value\n")
        result = _make_result("svc")
        with patch("driftwatch.cli.load_spec", return_value={"key": "value"}), \
             patch("driftwatch.cli.watch_all", return_value={"svc": {"key": "value"}}), \
             patch("driftwatch.cli.compare", return_value=result):
            code = run(["check", str(spec_file), "--targets", "targets.yaml"])
        assert code == 0

    def test_fail_on_drift_returns_one(self, tmp_path):
        spec_file = tmp_path / "svc.yaml"
        spec_file.write_text("key: value\n")
        result = _make_result("svc", missing=["key"])
        with patch("driftwatch.cli.load_spec", return_value={"key": "value"}), \
             patch("driftwatch.cli.watch_all", return_value={"svc": {}}), \
             patch("driftwatch.cli.compare", return_value=result):
            code = run(["check", str(spec_file), "--targets", "targets.yaml", "--fail-on-drift"])
        assert code == 1

    def test_spec_load_error_returns_two(self, tmp_path):
        spec_file = tmp_path / "bad.yaml"
        spec_file.write_text("")
        with patch("driftwatch.cli.load_spec", side_effect=SpecLoadError("bad file")):
            code = run(["check", str(spec_file), "--targets", "targets.yaml"])
        assert code == 2

    def test_watch_error_returns_two(self, tmp_path):
        spec_file = tmp_path / "svc.yaml"
        spec_file.write_text("key: value\n")
        with patch("driftwatch.cli.load_spec", return_value={"key": "value"}), \
             patch("driftwatch.cli.watch_all", side_effect=WatchError("timeout")):
            code = run(["check", str(spec_file), "--targets", "targets.yaml"])
        assert code == 2
