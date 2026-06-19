"""
Unit Tests for Data Quality Framework
=====================================
"""

import uuid
import pytest

from src.quality import (
    QualityChecker,
    RequiredFieldRule,
    TypeRule,
    RangeRule,
    PatternRule,
    EnumRule,
    CustomRule,
    Severity,
    email_rule,
    uuid_rule,
    create_cdc_event_checker,
)


class TestRequiredFieldRule:
    """Tests for RequiredFieldRule."""

    def test_passes_when_field_present(self):
        """Field is present and not null."""
        rule = RequiredFieldRule("name")
        result = rule.check({"name": "John"})
        
        assert result.passed is True
        assert result.message == ""

    def test_fails_when_field_null(self):
        """Field is null."""
        rule = RequiredFieldRule("name")
        result = rule.check({"name": None})
        
        assert result.passed is False
        assert "null" in result.message

    def test_fails_when_field_missing(self):
        """Field doesn't exist."""
        rule = RequiredFieldRule("name")
        result = rule.check({})
        
        assert result.passed is False

    def test_empty_string_passes(self):
        """Empty string is not null."""
        rule = RequiredFieldRule("name")
        result = rule.check({"name": ""})
        
        assert result.passed is True


class TestTypeRule:
    """Tests for TypeRule."""

    def test_passes_correct_type(self):
        """Value has correct type."""
        rule = TypeRule("age", int)
        result = rule.check({"age": 25})
        
        assert result.passed is True

    def test_fails_wrong_type(self):
        """Value has wrong type."""
        rule = TypeRule("age", int)
        result = rule.check({"age": "twenty-five"})
        
        assert result.passed is False
        assert "type" in result.message.lower()

    def test_null_allowed_by_default(self):
        """Null values pass by default."""
        rule = TypeRule("age", int)
        result = rule.check({"age": None})
        
        assert result.passed is True

    def test_null_not_allowed(self):
        """Null values fail when allow_null=False."""
        rule = TypeRule("age", int, allow_null=False)
        result = rule.check({"age": None})
        
        assert result.passed is False

    def test_multiple_types(self):
        """Value can be one of multiple types."""
        rule = TypeRule("amount", (int, float))
        
        assert rule.check({"amount": 100}).passed is True
        assert rule.check({"amount": 99.99}).passed is True
        assert rule.check({"amount": "100"}).passed is False


class TestRangeRule:
    """Tests for RangeRule."""

    def test_within_range(self):
        """Value within range passes."""
        rule = RangeRule("age", min_value=0, max_value=150)
        result = rule.check({"age": 25})
        
        assert result.passed is True

    def test_below_minimum(self):
        """Value below minimum fails."""
        rule = RangeRule("age", min_value=0)
        result = rule.check({"age": -5})
        
        assert result.passed is False
        assert "below minimum" in result.message

    def test_above_maximum(self):
        """Value above maximum fails."""
        rule = RangeRule("age", max_value=150)
        result = rule.check({"age": 200})
        
        assert result.passed is False
        assert "above maximum" in result.message

    def test_null_passes(self):
        """Null values pass (handled by RequiredFieldRule)."""
        rule = RangeRule("age", min_value=0, max_value=150)
        result = rule.check({"age": None})
        
        assert result.passed is True


class TestPatternRule:
    """Tests for PatternRule."""

    def test_matches_pattern(self):
        """Value matches pattern."""
        rule = PatternRule("code", r"^[A-Z]{3}\d{3}$", "code format")
        result = rule.check({"code": "ABC123"})
        
        assert result.passed is True

    def test_no_match(self):
        """Value doesn't match pattern."""
        rule = PatternRule("code", r"^[A-Z]{3}\d{3}$", "code format")
        result = rule.check({"code": "invalid"})
        
        assert result.passed is False


class TestEnumRule:
    """Tests for EnumRule."""

    def test_valid_value(self):
        """Value in allowed list."""
        rule = EnumRule("status", ["active", "inactive", "pending"])
        result = rule.check({"status": "active"})
        
        assert result.passed is True

    def test_invalid_value(self):
        """Value not in allowed list."""
        rule = EnumRule("status", ["active", "inactive", "pending"])
        result = rule.check({"status": "deleted"})
        
        assert result.passed is False


class TestCustomRule:
    """Tests for CustomRule."""

    def test_custom_check_passes(self):
        """Custom function returns True."""
        rule = CustomRule(
            name="total_positive",
            check_fn=lambda d: d.get("total", 0) > 0,
            error_message="Total must be positive",
        )
        result = rule.check({"total": 100})
        
        assert result.passed is True

    def test_custom_check_fails(self):
        """Custom function returns False."""
        rule = CustomRule(
            name="total_positive",
            check_fn=lambda d: d.get("total", 0) > 0,
            error_message="Total must be positive",
        )
        result = rule.check({"total": -5})
        
        assert result.passed is False
        assert result.message == "Total must be positive"


class TestHelperFunctions:
    """Tests for email_rule and uuid_rule helpers."""

    def test_valid_email(self):
        """Valid email passes."""
        rule = email_rule("email")
        result = rule.check({"email": "test@example.com"})
        
        assert result.passed is True

    def test_invalid_email(self):
        """Invalid email fails."""
        rule = email_rule("email")
        result = rule.check({"email": "not-an-email"})
        
        assert result.passed is False

    def test_valid_uuid(self):
        """Valid UUID passes."""
        rule = uuid_rule("id")
        result = rule.check({"id": str(uuid.uuid4())})
        
        assert result.passed is True

    def test_invalid_uuid(self):
        """Invalid UUID fails."""
        rule = uuid_rule("id")
        result = rule.check({"id": "not-a-uuid"})
        
        assert result.passed is False


class TestQualityChecker:
    """Tests for QualityChecker."""

    def test_all_rules_pass(self):
        """All rules pass."""
        checker = QualityChecker(name="test")
        checker.add_rule(RequiredFieldRule("id"))
        checker.add_rule(TypeRule("id", int))
        
        report = checker.check({"id": 123})
        
        assert report.passed is True
        assert len(report.failures) == 0

    def test_some_rules_fail(self):
        """Some rules fail."""
        checker = QualityChecker(name="test")
        checker.add_rule(RequiredFieldRule("id"))
        checker.add_rule(RequiredFieldRule("name"))
        
        report = checker.check({"id": 123})
        
        assert report.passed is False
        assert len(report.failures) == 1

    def test_report_to_dict(self):
        """Report converts to dictionary."""
        checker = QualityChecker(name="test")
        checker.add_rule(RequiredFieldRule("id"))
        
        report = checker.check({"id": None}, event_id="test-123")
        result_dict = report.to_dict()
        
        assert result_dict["event_id"] == "test-123"
        assert result_dict["passed"] is False
        assert len(result_dict["failure_details"]) == 1

    def test_chaining(self):
        """Rules can be added with chaining."""
        checker = (
            QualityChecker(name="test")
            .add_rule(RequiredFieldRule("id"))
            .add_rule(RequiredFieldRule("name"))
        )
        
        assert len(checker.rules) == 2


class TestCDCEventChecker:
    """Tests for the pre-built CDC event checker."""

    def test_valid_insert_event(self):
        """Valid INSERT event passes."""
        checker = create_cdc_event_checker()
        
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INSERT",
            "source": {
                "database": "source_db",
                "schema": "public",
                "table": "customers",
            },
            "before": None,
            "after": {"id": 1, "name": "John"},
        }
        
        report = checker.check(event)
        assert report.passed is True

    def test_insert_without_after_fails(self):
        """INSERT without 'after' data fails."""
        checker = create_cdc_event_checker()
        
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INSERT",
            "source": {
                "database": "source_db",
                "schema": "public",
                "table": "customers",
            },
            "before": None,
            "after": None,
        }
        
        report = checker.check(event)
        assert report.passed is False
        assert any("after" in f.message.lower() for f in report.failures)

    def test_invalid_operation_fails(self):
        """Invalid operation type fails."""
        checker = create_cdc_event_checker()
        
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INVALID",
            "source": {
                "database": "source_db",
                "schema": "public",
                "table": "customers",
            },
        }
        
        report = checker.check(event)
        assert report.passed is False
