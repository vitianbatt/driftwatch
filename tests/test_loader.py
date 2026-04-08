"""Tests for driftwatch.loader — spec file loading."""

import json
import textwrap
from pathlib import Path

import pytest
import yaml

from driftwatch.loader import SpecLoadError, load_spec, load_specs_from_dir

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# load_spec
# ---------------------------------------------------------------------------

class TestLoadSpec:
    def test_load_yaml_fixture(self):
        spec = load_spec(FIXTURES / "sample_spec.yaml")
        assert spec["service"] == "payment-api"
        assert spec["replicas"] == 3
        assert spec["env"]["LOG_LEVEL"] == "info"

    def test_load_json_file(self, tmp_path):
        data = {"service": "auth", "replicas": 2}
        spec_file = tmp_path / "auth.json"
        spec_file.write_text(json.dumps(data))
        assert load_spec(spec_file) == data

    def test_load_yml_extension(self, tmp_path):
        spec_file = tmp_path / "svc.yml"
        spec_file.write_text("service: svc\nreplicas: 1\n")
        spec = load_spec(spec_file)
        assert spec["service"] == "svc"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(SpecLoadError, match="not found"):
            load_spec(tmp_path / "nonexistent.yaml")

    def test_unsupported_extension_raises(self, tmp_path):
        bad = tmp_path / "spec.toml"
        bad.write_text("[service]\nname = 'x'\n")
        with pytest.raises(SpecLoadError, match="Unsupported file format"):
            load_spec(bad)

    def test_malformed_yaml_raises(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("key: [unclosed")
        with pytest.raises(SpecLoadError, match="Failed to parse"):
            load_spec(bad)

    def test_malformed_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json}")
        with pytest.raises(SpecLoadError, match="Failed to parse"):
            load_spec(bad)

    def test_non_dict_top_level_raises(self, tmp_path):
        bad = tmp_path / "list.yaml"
        bad.write_text("- item1\n- item2\n")
        with pytest.raises(SpecLoadError, match="must contain a YAML/JSON object"):
            load_spec(bad)


# ---------------------------------------------------------------------------
# load_specs_from_dir
# ---------------------------------------------------------------------------

class TestLoadSpecsFromDir:
    def test_loads_multiple_files(self, tmp_path):
        (tmp_path / "alpha.yaml").write_text("service: alpha\n")
        (tmp_path / "beta.json").write_text(json.dumps({"service": "beta"}))
        (tmp_path / "ignore.txt").write_text("not a spec")

        specs = load_specs_from_dir(tmp_path)
        assert set(specs.keys()) == {"alpha", "beta"}
        assert specs["alpha"]["service"] == "alpha"

    def test_empty_directory(self, tmp_path):
        assert load_specs_from_dir(tmp_path) == {}

    def test_invalid_directory_raises(self, tmp_path):
        with pytest.raises(SpecLoadError, match="Not a directory"):
            load_specs_from_dir(tmp_path / "no_such_dir")
