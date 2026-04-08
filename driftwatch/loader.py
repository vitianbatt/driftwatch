"""Spec loader for driftwatch — loads YAML/JSON service spec files."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import yaml


class SpecLoadError(Exception):
    """Raised when a spec file cannot be loaded or parsed."""


def load_spec(path: str | Path) -> Dict[str, Any]:
    """Load a single YAML or JSON spec file and return its contents as a dict.

    Parameters
    ----------
    path:
        Filesystem path to the spec file.  Supports ``.yaml``, ``.yml``,
        and ``.json`` extensions.

    Raises
    ------
    SpecLoadError
        If the file does not exist, has an unsupported extension, or cannot
        be parsed.
    """
    p = Path(path)
    if not p.exists():
        raise SpecLoadError(f"Spec file not found: {path}")

    suffix = p.suffix.lower()
    if suffix not in (".yaml", ".yml", ".json"):
        raise SpecLoadError(
            f"Unsupported file extension '{suffix}'. Expected .yaml, .yml, or .json."
        )

    try:
        with p.open("r", encoding="utf-8") as fh:
            if suffix == ".json":
                data = json.load(fh)
            else:
                data = yaml.safe_load(fh)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise SpecLoadError(f"Failed to parse spec file '{path}': {exc}") from exc

    if not isinstance(data, dict):
        raise SpecLoadError(
            f"Spec file '{path}' must contain a YAML/JSON mapping at the top level."
        )
    return data


def load_specs_from_dir(directory: str | Path) -> Dict[str, Dict[str, Any]]:
    """Load all YAML/JSON spec files from *directory*.

    Returns a mapping of ``{filename_stem: spec_dict}``.

    Raises
    ------
    SpecLoadError
        If *directory* does not exist or is not a directory.
    """
    d = Path(directory)
    if not d.exists():
        raise SpecLoadError(f"Spec directory not found: {directory}")
    if not d.is_dir():
        raise SpecLoadError(f"Path is not a directory: {directory}")

    specs: Dict[str, Dict[str, Any]] = {}
    for entry in sorted(d.iterdir()):
        if entry.is_file() and entry.suffix.lower() in (".yaml", ".yml", ".json"):
            specs[entry.stem] = load_spec(entry)
    return specs


def load_notifier_config(path: str | Path) -> Dict[str, Any]:
    """Convenience wrapper — load a notifier config YAML file.

    Returns the raw dict so callers can construct a ``NotifierConfig``.
    """
    data = load_spec(path)
    return data
