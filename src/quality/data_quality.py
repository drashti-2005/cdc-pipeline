"""
Data Quality Framework
======================
Validates CDC events before processing to catch bad data early.

WHY DATA QUALITY?
-----------------
Bad data causes:
1. Pipeline failures (crashes, stuck jobs)
2. Wrong analytics (garbage in = garbage out)
3. Customer issues (wrong invoices, missing orders)
4. Hard-to-debug problems (bad data propagates downstream)

The earlier you catch bad data, the cheaper it is to fix.
This is called "shift-left" - move quality checks earlier in the pipeline.

OUR APPROACH
------------
1. Define RULES for what "good data" looks like
2. Check each event against rules BEFORE processing
3. Bad events go to Dead Letter Queue (DLQ) for investigation
4. Track quality METRICS for monitoring

RULE TYPES
----------
- Required fields: Must not be null
- Type checks: Must be correct type (int, string, etc.)
- Range checks: Numbers within valid range
- Format checks: Strings match pattern (email, UUID, etc.)
- Business rules: Custom validation (e.g., order total > 0)

FOR INTERVIEWS
--------------
Q: How do you handle data quality in a pipeline?
A: Three-pronged approach:
   1. Validate at ingestion (reject bad data early)
   2. Monitor quality metrics (track issues over time)
   3. DLQ for failed events (investigate and replay)
"""

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional

from metrics import (
    DATA_QUALITY_CHECKS_TOTAL,
    DATA_QUALITY_FAILURES_TOTAL,
)

logger = logging.getLogger(__name__)


# ============================================================
# Rule Severity Levels
# ============================================================

class Severity(str, Enum):
    """How serious is a rule violation?
    
    WARNING: Log it, but continue processing
    ERROR: Send to DLQ, don't process
    CRITICAL: Stop pipeline, alert on-call
    """
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# ============================================================
# Rule Result
# ============================================================

@dataclass
class RuleResult:
    """Result of checking a single rule."""
    rule_name: str
    passed: bool
    message: str = ""
    severity: Severity = Severity.ERROR
    field_name: Optional[str] = None
    actual_value: Any = None
    expected: str = ""


@dataclass
class QualityReport:
    """Complete quality check report for an event."""
    event_id: str
    timestamp: datetime
    passed: bool
    results: list[RuleResult] = field(default_factory=list)
    
    @property
    def failures(self) -> list[RuleResult]:
        """Get only failed rules."""
        return [r for r in self.results if not r.passed]
    
    @property
    def warnings(self) -> list[RuleResult]:
        """Get warning-level failures."""
        return [r for r in self.failures if r.severity == Severity.WARNING]
    
    @property
    def errors(self) -> list[RuleResult]:
        """Get error-level failures."""
        return [r for r in self.failures if r.severity == Severity.ERROR]
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging/storage."""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "passed": self.passed,
            "total_rules": len(self.results),
            "failures": len(self.failures),
            "failure_details": [
                {
                    "rule": r.rule_name,
                    "field": r.field_name,
                    "message": r.message,
                    "severity": r.severity.value,
                }
                for r in self.failures
            ],
        }


# ============================================================
# Base Rule Class
# ============================================================

class DataQualityRule(ABC):
    """
    Base class for all data quality rules.
    
    SIMPLE EXPLANATION:
    A rule is like a test question for your data:
    - "Is email not null?" → Pass/Fail
    - "Is age between 0 and 150?" → Pass/Fail
    - "Does order_id match UUID format?" → Pass/Fail
    
    To create a new rule:
    1. Inherit from DataQualityRule
    2. Implement the check() method
    3. Return RuleResult with pass/fail and message
    """
    
    def __init__(
        self,
        name: str,
        severity: Severity = Severity.ERROR,
        description: str = "",
    ):
        self.name = name
        self.severity = severity
        self.description = description
    
    @abstractmethod
    def check(self, data: dict) -> RuleResult:
        """
        Check if data passes this rule.
        
        Args:
            data: Dictionary to validate
            
        Returns:
            RuleResult with pass/fail status
        """
        pass


# ============================================================
# Common Rules
# ============================================================

class RequiredFieldRule(DataQualityRule):
    """Check that a field exists and is not null."""
    
    def __init__(
        self,
        field_name: str,
        severity: Severity = Severity.ERROR,
    ):
        super().__init__(
            name=f"required_{field_name}",
            severity=severity,
            description=f"Field '{field_name}' must not be null",
        )
        self.field_name = field_name
    
    def check(self, data: dict) -> RuleResult:
        value = data.get(self.field_name)
        passed = value is not None
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message="" if passed else f"Required field '{self.field_name}' is null",
            severity=self.severity,
            field_name=self.field_name,
            actual_value=value,
            expected="not null",
        )


class TypeRule(DataQualityRule):
    """Check that a field has the correct type."""
    
    def __init__(
        self,
        field_name: str,
        expected_type: type,
        severity: Severity = Severity.ERROR,
        allow_null: bool = True,
    ):
        # Handle tuple of types for description
        if isinstance(expected_type, tuple):
            type_names = " or ".join(t.__name__ for t in expected_type)
        else:
            type_names = expected_type.__name__
        
        super().__init__(
            name=f"type_{field_name}",
            severity=severity,
            description=f"Field '{field_name}' must be {type_names}",
        )
        self.field_name = field_name
        self.expected_type = expected_type
        self.allow_null = allow_null
        self._type_names = type_names
    
    def check(self, data: dict) -> RuleResult:
        value = data.get(self.field_name)
        
        # Handle null values
        if value is None:
            passed = self.allow_null
            message = "" if passed else f"Field '{self.field_name}' is null"
        else:
            passed = isinstance(value, self.expected_type)
            message = "" if passed else (
                f"Field '{self.field_name}' has type {type(value).__name__}, "
                f"expected {self._type_names}"
            )
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            severity=self.severity,
            field_name=self.field_name,
            actual_value=value,
            expected=self._type_names,
        )


class RangeRule(DataQualityRule):
    """Check that a numeric field is within a valid range."""
    
    def __init__(
        self,
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: Severity = Severity.ERROR,
    ):
        range_str = f"[{min_value or '-∞'}, {max_value or '∞'}]"
        super().__init__(
            name=f"range_{field_name}",
            severity=severity,
            description=f"Field '{field_name}' must be in range {range_str}",
        )
        self.field_name = field_name
        self.min_value = min_value
        self.max_value = max_value
    
    def check(self, data: dict) -> RuleResult:
        value = data.get(self.field_name)
        
        if value is None:
            return RuleResult(
                rule_name=self.name,
                passed=True,  # Null handled by RequiredFieldRule
                field_name=self.field_name,
            )
        
        passed = True
        message = ""
        
        if self.min_value is not None and value < self.min_value:
            passed = False
            message = f"Field '{self.field_name}' value {value} is below minimum {self.min_value}"
        elif self.max_value is not None and value > self.max_value:
            passed = False
            message = f"Field '{self.field_name}' value {value} is above maximum {self.max_value}"
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message=message,
            severity=self.severity,
            field_name=self.field_name,
            actual_value=value,
            expected=f"[{self.min_value}, {self.max_value}]",
        )


class PatternRule(DataQualityRule):
    """Check that a string field matches a regex pattern."""
    
    def __init__(
        self,
        field_name: str,
        pattern: str,
        pattern_name: str = "pattern",
        severity: Severity = Severity.ERROR,
    ):
        super().__init__(
            name=f"pattern_{field_name}",
            severity=severity,
            description=f"Field '{field_name}' must match {pattern_name}",
        )
        self.field_name = field_name
        self.pattern = re.compile(pattern)
        self.pattern_name = pattern_name
    
    def check(self, data: dict) -> RuleResult:
        value = data.get(self.field_name)
        
        if value is None:
            return RuleResult(
                rule_name=self.name,
                passed=True,
                field_name=self.field_name,
            )
        
        passed = bool(self.pattern.match(str(value)))
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message="" if passed else f"Field '{self.field_name}' doesn't match {self.pattern_name}",
            severity=self.severity,
            field_name=self.field_name,
            actual_value=value,
            expected=self.pattern_name,
        )


class EnumRule(DataQualityRule):
    """Check that a field value is one of allowed values."""
    
    def __init__(
        self,
        field_name: str,
        allowed_values: list[Any],
        severity: Severity = Severity.ERROR,
    ):
        super().__init__(
            name=f"enum_{field_name}",
            severity=severity,
            description=f"Field '{field_name}' must be one of {allowed_values}",
        )
        self.field_name = field_name
        self.allowed_values = set(allowed_values)
    
    def check(self, data: dict) -> RuleResult:
        value = data.get(self.field_name)
        
        if value is None:
            return RuleResult(
                rule_name=self.name,
                passed=True,
                field_name=self.field_name,
            )
        
        passed = value in self.allowed_values
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message="" if passed else f"Field '{self.field_name}' value '{value}' not in allowed values",
            severity=self.severity,
            field_name=self.field_name,
            actual_value=value,
            expected=str(list(self.allowed_values)),
        )


class CustomRule(DataQualityRule):
    """Custom rule using a lambda function."""
    
    def __init__(
        self,
        name: str,
        check_fn: Callable[[dict], bool],
        error_message: str,
        severity: Severity = Severity.ERROR,
    ):
        super().__init__(
            name=name,
            severity=severity,
            description=error_message,
        )
        self.check_fn = check_fn
        self.error_message = error_message
    
    def check(self, data: dict) -> RuleResult:
        try:
            passed = self.check_fn(data)
        except Exception as e:
            passed = False
            logger.warning(f"Custom rule '{self.name}' raised exception: {e}")
        
        return RuleResult(
            rule_name=self.name,
            passed=passed,
            message="" if passed else self.error_message,
            severity=self.severity,
        )


# ============================================================
# Common Patterns
# ============================================================

# Pre-compiled regex patterns for common validations
PATTERNS = {
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    "phone": r"^\+?[1-9]\d{1,14}$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "datetime_iso": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
}


def email_rule(field_name: str, severity: Severity = Severity.ERROR) -> PatternRule:
    """Create an email validation rule."""
    return PatternRule(field_name, PATTERNS["email"], "email format", severity)


def uuid_rule(field_name: str, severity: Severity = Severity.ERROR) -> PatternRule:
    """Create a UUID validation rule."""
    return PatternRule(field_name, PATTERNS["uuid"], "UUID format", severity)


# ============================================================
# Quality Checker
# ============================================================

class QualityChecker:
    """
    Runs data quality checks on events.
    
    SIMPLE EXPLANATION:
    Like a quality control inspector at a factory:
    1. Define what "good" looks like (rules)
    2. Check each item against the rules
    3. Pass/fail items and record results
    4. Track metrics for continuous improvement
    
    USAGE:
        checker = QualityChecker()
        checker.add_rule(RequiredFieldRule("id"))
        checker.add_rule(email_rule("email"))
        
        report = checker.check(event)
        if not report.passed:
            send_to_dlq(event, report)
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.rules: list[DataQualityRule] = []
    
    def add_rule(self, rule: DataQualityRule) -> "QualityChecker":
        """Add a rule to the checker. Returns self for chaining."""
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules: list[DataQualityRule]) -> "QualityChecker":
        """Add multiple rules. Returns self for chaining."""
        self.rules.extend(rules)
        return self
    
    def check(self, data: dict, event_id: str = "") -> QualityReport:
        """
        Run all rules against the data.
        
        Args:
            data: Dictionary to validate
            event_id: Optional event ID for the report
            
        Returns:
            QualityReport with all results
        """
        results = []
        all_passed = True
        
        for rule in self.rules:
            try:
                result = rule.check(data)
                results.append(result)
                
                # Track metrics
                DATA_QUALITY_CHECKS_TOTAL.labels(
                    checker=self.name,
                    rule=rule.name,
                ).inc()
                
                if not result.passed:
                    all_passed = False
                    DATA_QUALITY_FAILURES_TOTAL.labels(
                        checker=self.name,
                        rule=rule.name,
                        severity=result.severity.value,
                    ).inc()
                    
                    logger.warning(
                        f"Quality check failed: {result.message}",
                        extra={
                            "rule": rule.name,
                            "event_id": event_id,
                            "severity": result.severity.value,
                        },
                    )
            except Exception as e:
                logger.error(f"Rule '{rule.name}' raised exception: {e}")
                results.append(RuleResult(
                    rule_name=rule.name,
                    passed=False,
                    message=f"Rule execution error: {e}",
                    severity=Severity.ERROR,
                ))
                all_passed = False
        
        return QualityReport(
            event_id=event_id or data.get("event_id", "unknown"),
            timestamp=datetime.now(timezone.utc),
            passed=all_passed,
            results=results,
        )


# ============================================================
# Pre-built Checkers for CDC Events
# ============================================================

def create_cdc_event_checker() -> QualityChecker:
    """
    Create a quality checker for CDC events.
    
    Validates the structure of events from our pipeline.
    """
    checker = QualityChecker(name="cdc_event")
    
    # Required fields
    checker.add_rule(RequiredFieldRule("event_id"))
    checker.add_rule(RequiredFieldRule("operation"))
    checker.add_rule(RequiredFieldRule("source"))
    
    # Event ID format
    checker.add_rule(uuid_rule("event_id"))
    
    # Operation must be valid
    checker.add_rule(EnumRule("operation", ["INSERT", "UPDATE", "DELETE"]))
    
    # Source must have required fields (nested check)
    checker.add_rule(CustomRule(
        name="source_has_table",
        check_fn=lambda d: d.get("source", {}).get("table") is not None,
        error_message="source.table is required",
    ))
    
    checker.add_rule(CustomRule(
        name="source_has_database",
        check_fn=lambda d: d.get("source", {}).get("database") is not None,
        error_message="source.database is required",
    ))
    
    # Business rules
    checker.add_rule(CustomRule(
        name="insert_has_after",
        check_fn=lambda d: d.get("operation") != "INSERT" or d.get("after") is not None,
        error_message="INSERT operation must have 'after' data",
    ))
    
    checker.add_rule(CustomRule(
        name="delete_has_before",
        check_fn=lambda d: d.get("operation") != "DELETE" or d.get("before") is not None,
        error_message="DELETE operation must have 'before' data",
    ))
    
    return checker


def create_customer_checker() -> QualityChecker:
    """Create a quality checker for customer data."""
    checker = QualityChecker(name="customer")
    
    checker.add_rule(RequiredFieldRule("id"))
    checker.add_rule(RequiredFieldRule("email"))
    checker.add_rule(TypeRule("id", int))
    checker.add_rule(email_rule("email"))
    checker.add_rule(RangeRule("id", min_value=1))
    
    return checker


def create_order_checker() -> QualityChecker:
    """Create a quality checker for order data."""
    checker = QualityChecker(name="order")
    
    checker.add_rule(RequiredFieldRule("id"))
    checker.add_rule(RequiredFieldRule("customer_id"))
    checker.add_rule(RequiredFieldRule("total_amount"))
    checker.add_rule(TypeRule("id", int))
    checker.add_rule(TypeRule("customer_id", int))
    checker.add_rule(TypeRule("total_amount", (int, float)))
    checker.add_rule(RangeRule("total_amount", min_value=0))
    
    return checker
