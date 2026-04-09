"""Tests for validator module."""

import pytest
from driftwatch.validator import (
    ValidationError,
    ValidationRule,
    ValidationResult,
    validate_field,
    validate_spec,
)


class TestValidationRule:
    def test_valid_rule_created(self):
        rule = ValidationRule(field="name", required=True)
        assert rule.field == "name"
        assert rule.required is True

    def test_empty_field_raises(self):
        with pytest.raises(ValidationError, match="Field name cannot be empty"):
            ValidationRule(field="")

    def test_whitespace_field_raises(self):
        with pytest.raises(ValidationError, match="Field name cannot be empty"):
            ValidationRule(field="   ")

    def test_invalid_regex_pattern_raises(self):
        with pytest.raises(ValidationError, match="Invalid regex pattern"):
            ValidationRule(field="name", pattern="[invalid")

    def test_valid_regex_pattern_accepted(self):
        rule = ValidationRule(field="name", pattern=r"^[a-z]+$")
        assert rule.pattern == r"^[a-z]+$"


class TestValidationResult:
    def test_no_errors_is_valid(self):
        result = ValidationResult(valid=True, errors=[], warnings=[])
        assert result.valid is True
        assert not result.has_errors()

    def test_with_errors_has_errors(self):
        result = ValidationResult(valid=False, errors=["error"], warnings=[])
        assert result.has_errors() is True

    def test_with_warnings_has_warnings(self):
        result = ValidationResult(valid=True, errors=[], warnings=["warning"])
        assert result.has_warnings() is True


class TestValidateField:
    def test_pattern_match_valid(self):
        rule = ValidationRule(field="name", pattern=r"^[a-z]+$")
        errors = validate_field("hello", rule)
        assert errors == []

    def test_pattern_match_invalid(self):
        rule = ValidationRule(field="name", pattern=r"^[a-z]+$")
        errors = validate_field("Hello123", rule)
        assert len(errors) == 1
        assert "does not match pattern" in errors[0]

    def test_allowed_values_valid(self):
        rule = ValidationRule(field="env", allowed_values=["dev", "prod"])
        errors = validate_field("dev", rule)
        assert errors == []

    def test_allowed_values_invalid(self):
        rule = ValidationRule(field="env", allowed_values=["dev", "prod"])
        errors = validate_field("staging", rule)
        assert len(errors) == 1
        assert "not in allowed values" in errors[0]

    def test_min_value_valid(self):
        rule = ValidationRule(field="port", min_value=1024)
        errors = validate_field(8080, rule)
        assert errors == []

    def test_min_value_invalid(self):
        rule = ValidationRule(field="port", min_value=1024)
        errors = validate_field(80, rule)
        assert len(errors) == 1
        assert "less than minimum" in errors[0]

    def test_max_value_valid(self):
        rule = ValidationRule(field="port", max_value=65535)
        errors = validate_field(8080, rule)
        assert errors == []

    def test_max_value_invalid(self):
        rule = ValidationRule(field="port", max_value=65535)
        errors = validate_field(70000, rule)
        assert len(errors) == 1
        assert "exceeds maximum" in errors[0]


class TestValidateSpec:
    def test_valid_spec_passes(self):
        spec = {"name": "service", "port": 8080}
        rules = [
            ValidationRule(field="name", required=True),
            ValidationRule(field="port", required=True),
        ]
        result = validate_spec(spec, rules)
        assert result.valid is True
        assert result.errors == []

    def test_missing_required_field_fails(self):
        spec = {"name": "service"}
        rules = [ValidationRule(field="port", required=True)]
        result = validate_spec(spec, rules)
        assert result.valid is False
        assert len(result.errors) == 1
        assert "Required field 'port' is missing" in result.errors[0]

    def test_missing_optional_field_passes(self):
        spec = {"name": "service"}
        rules = [ValidationRule(field="port", required=False)]
        result = validate_spec(spec, rules)
        assert result.valid is True

    def test_multiple_validation_errors(self):
        spec = {"env": "staging", "port": 70000}
        rules = [
            ValidationRule(field="env", allowed_values=["dev", "prod"]),
            ValidationRule(field="port", max_value=65535),
        ]
        result = validate_spec(spec, rules)
        assert result.valid is False
        assert len(result.errors) == 2
