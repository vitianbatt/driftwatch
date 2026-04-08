"""Command-line interface for driftwatch."""

import sys
import argparse
from pathlib import Path

from driftwatch.loader import load_spec, load_specs_from_dir, SpecLoadError
from driftwatch.watcher import watch_all, WatchError
from driftwatch.comparator import compare
from driftwatch.reporter import generate_report, OutputFormat, ReportError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="driftwatch",
        description="Detect configuration drift between deployed services and declared specs.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="Check for drift against a spec file or directory.")
    check.add_argument(
        "spec",
        metavar="SPEC",
        help="Path to a spec file (.yaml/.json) or directory of specs.",
    )
    check.add_argument(
        "--targets",
        metavar="TARGETS",
        required=True,
        help="Path to the targets YAML file listing live service endpoints.",
    )
    check.add_argument(
        "--format",
        dest="output_format",
        choices=[f.value for f in OutputFormat],
        default=OutputFormat.TEXT.value,
        help="Output format (default: text).",
    )
    check.add_argument(
        "--fail-on-drift",
        action="store_true",
        default=False,
        help="Exit with code 1 if any drift is detected.",
    )

    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "check":
        spec_path = Path(args.spec)
        try:
            if spec_path.is_dir():
                specs = load_specs_from_dir(spec_path)
            else:
                specs = {spec_path.stem: load_spec(spec_path)}
        except SpecLoadError as exc:
            print(f"[driftwatch] Error loading spec: {exc}", file=sys.stderr)
            return 2

        try:
            live_configs = watch_all(args.targets)
        except WatchError as exc:
            print(f"[driftwatch] Error fetching live configs: {exc}", file=sys.stderr)
            return 2

        results = [
            compare(service, specs.get(service, {}), live)
            for service, live in live_configs.items()
        ]

        try:
            fmt = OutputFormat(args.output_format)
            report = generate_report(results, fmt)
        except ReportError as exc:
            print(f"[driftwatch] Error generating report: {exc}", file=sys.stderr)
            return 2

        print(report)

        if args.fail_on_drift and any(r.has_drift() for r in results):
            return 1

    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
