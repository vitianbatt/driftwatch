"""Loads and parses service spec files (YAML/JSON) for drift comparison."""

import json
import os
from pathlib import Path
from typing import Any

import yaml


class SpecLoadError(Exception):
    """Raised when a spec file cannot be loaded or parsed."""


def load_spec(path: str | Path) -> dict[str, Any]:
    """Load a service spec from a YAML or JSON file.

    Args:
        path: Path to the spec file.

    Returns:
        Parsed spec as a dictionary.

    Raises:
        SpecLoadError: If the file is missing, unreadable, or malformed.
    """
    path = Path(path)

    if not path.exists():
        raise SpecLoadError(f"Spec file not found: {path}")

    if not path.is_file():
        raise SpecLoadError(f"Path is not a file: {path}")

    suffix = path.suffix.lower()
    if suffix not in (".yaml", ".yml", ".json"):
        raise SpecLoadError(
            f"Unsupported file format '{suffix}'. Expected .yaml, .yml, or .json."
        )

    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
    except OSError as exc:
        raise SpecLoadError(f"Cannot read spec file: {exc}") from exc

    try:
        if suffix == ".json":
            data = json.loads(raw)
        else:
            data = yaml.safe_load(raw)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise SpecLoadError(f"Failed to parse spec file '{path}': {exc}") from exc

    if not isinstance(data, dict):
        raise SpecLoadError(
            f"Spec file '{path}' must contain a YAML/JSON object at the top level."
        )

    return data


def load_specs_from_dir(directory: str | Path) -> dict[str, dict[str, Any]]:
    """Load all spec files from a directory.

    Returns:
        Mapping of stem name -> parsed spec dict.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise SpecLoadError(f"Not a directory: {directory}")

    specs: dict[str, dict[str, Any]] = {}
    for entry in sorted(directory.iterdir()):
        if entry.suffix.lower() in (".yaml", ".yml", ".json"):
            specs[entry.stem] = load_spec(entry)
    return specs
