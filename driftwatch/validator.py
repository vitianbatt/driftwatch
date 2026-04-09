"""Validation module for spec files and configuration."""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


@dataclass
class ValidationRule:
    """Defines a validation rule for configuration fields."""
    field: str
    required: bool = False
    pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def __post_init__(self):
        if not self.field or not self.field.strip():
            raise ValidationError("Field name cannot be empty")
        
        if self.pattern:
            try:
                re.compile(self.pattern)
            except re.error as e:
                raise ValidationError(f"Invalid regex pattern: {e}")


@dataclass
class ValidationResult:
    """Result of validation operation."""
    valid: bool
    errors: List[str]
    warnings: List[str]

    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0


def validate_field(value: Any, rule: ValidationRule) -> List[str]:
    """Validate a single field against a rule.
    
    Args:
        value: The value to validate
        rule: The validation rule to apply
        
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    
    if rule.pattern and isinstance(value, str):
        if not re.match(rule.pattern, value):
            errors.append(f"{rule.field}: does not match pattern {rule.pattern}")
    
    if rule.allowed_values is not None:
        if value not in rule.allowed_values:
            errors.append(f"{rule.field}: '{value}' not in allowed values {rule.allowed_values}")
    
    if rule.min_value is not None and isinstance(value, (int, float)):
        if value < rule.min_value:
            errors.append(f"{rule.field}: {value} is less than minimum {rule.min_value}")
    
    if rule.max_value is not None and isinstance(value, (int, float)):
        if value > rule.max_value:
            errors.append(f"{rule.field}: {value} exceeds maximum {rule.max_value}")
    
    return errors


def validate_spec(spec: Dict[str, Any], rules: List[ValidationRule]) -> ValidationResult:
    """Validate a spec against a set of rules.
    
    Args:
        spec: The specification dictionary to validate
        rules: List of validation rules to apply
        
    Returns:
        ValidationResult with validation outcome
    """
    errors = []
    warnings = []
    
    for rule in rules:
        if rule.field not in spec:
            if rule.required:
                errors.append(f"Required field '{rule.field}' is missing")
            continue
        
        field_errors = validate_field(spec[rule.field], rule)
        errors.extend(field_errors)
    
    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings
    )
