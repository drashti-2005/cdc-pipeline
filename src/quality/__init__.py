"""
Data Quality Module
===================
Validates CDC events to ensure data quality.

Usage:
    from .quality import QualityChecker, RequiredFieldRule
    
    checker = QualityChecker()
    checker.add_rule(RequiredFieldRule("id"))
    
    report = checker.check(event)
    if not report.passed:
        handle_bad_event(event, report)
"""

from .data_quality import (
    # Core classes
    DataQualityRule,
    QualityChecker,
    QualityReport,
    RuleResult,
    Severity,
    
    # Built-in rules
    RequiredFieldRule,
    TypeRule,
    RangeRule,
    PatternRule,
    EnumRule,
    CustomRule,
    
    # Helper functions
    email_rule,
    uuid_rule,
    
    # Pre-built checkers
    create_cdc_event_checker,
    create_customer_checker,
    create_order_checker,
)

__all__ = [
    "DataQualityRule",
    "QualityChecker",
    "QualityReport",
    "RuleResult",
    "Severity",
    "RequiredFieldRule",
    "TypeRule",
    "RangeRule",
    "PatternRule",
    "EnumRule",
    "CustomRule",
    "email_rule",
    "uuid_rule",
    "create_cdc_event_checker",
    "create_customer_checker",
    "create_order_checker",
]
