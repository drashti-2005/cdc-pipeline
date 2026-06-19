"""
Schema Compatibility Checking

Implements Avro schema compatibility rules:
- BACKWARD: New schema can read old data
- FORWARD: Old schema can read new data  
- FULL: Both backward and forward compatible
- NONE: No compatibility checking

SIMPLE EXPLANATION:
Think of schemas like contracts:
- BACKWARD: "I promise to understand old messages"
- FORWARD: "I promise my messages work with old readers"
- FULL: "I promise both ways"
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
from enum import Enum

logger = logging.getLogger(__name__)


class CompatibilityLevel(Enum):
    """
    Schema compatibility levels.
    
    NONE: No compatibility checking
    BACKWARD: New schema can read data written by old schema
    FORWARD: Old schema can read data written by new schema
    FULL: Both backward and forward compatible
    BACKWARD_TRANSITIVE: Backward compatible with ALL previous versions
    FORWARD_TRANSITIVE: Forward compatible with ALL previous versions
    FULL_TRANSITIVE: Full compatible with ALL previous versions
    """
    
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


@dataclass
class CompatibilityIssue:
    """A specific compatibility problem."""
    
    field_name: str
    issue_type: str
    description: str
    severity: str = "error"  # error, warning
    
    def __str__(self) -> str:
        return f"[{self.severity.upper()}] {self.field_name}: {self.description}"


@dataclass
class CompatibilityResult:
    """Result of compatibility check."""
    
    is_compatible: bool
    level: CompatibilityLevel
    issues: List[CompatibilityIssue] = field(default_factory=list)
    
    @property
    def error_count(self) -> int:
        """Number of error-level issues."""
        return sum(1 for i in self.issues if i.severity == "error")
    
    @property
    def warning_count(self) -> int:
        """Number of warning-level issues."""
        return sum(1 for i in self.issues if i.severity == "warning")
    
    def summary(self) -> str:
        """Generate human-readable summary."""
        status = "COMPATIBLE" if self.is_compatible else "INCOMPATIBLE"
        lines = [
            f"Compatibility Check: {status}",
            f"Level: {self.level.value}",
            f"Errors: {self.error_count}, Warnings: {self.warning_count}",
        ]
        
        if self.issues:
            lines.append("Issues:")
            for issue in self.issues:
                lines.append(f"  - {issue}")
        
        return "\n".join(lines)


class CompatibilityChecker:
    """
    Check schema compatibility using Avro rules.
    
    AVRO COMPATIBILITY RULES:
    
    BACKWARD (can read old data):
    - Can add fields WITH defaults
    - Can remove fields
    - Cannot add required fields
    - Cannot change field types (mostly)
    
    FORWARD (old can read new data):
    - Can add fields
    - Can remove fields WITH defaults
    - Cannot remove required fields
    - Cannot change field types (mostly)
    
    FULL (both):
    - Can only add/remove fields with defaults
    """
    
    def __init__(self, level: CompatibilityLevel = CompatibilityLevel.BACKWARD):
        self.level = level
    
    def check(
        self,
        new_schema: Dict[str, Any],
        old_schema: Dict[str, Any],
    ) -> CompatibilityResult:
        """
        Check compatibility between schemas.
        
        Args:
            new_schema: The new schema to validate
            old_schema: The existing schema to check against
            
        Returns:
            CompatibilityResult with issues found
        """
        issues: List[CompatibilityIssue] = []
        
        if self.level == CompatibilityLevel.NONE:
            return CompatibilityResult(
                is_compatible=True,
                level=self.level,
                issues=[],
            )
        
        # Get fields from both schemas
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        
        # Check based on compatibility level
        if self.level in (
            CompatibilityLevel.BACKWARD,
            CompatibilityLevel.BACKWARD_TRANSITIVE,
            CompatibilityLevel.FULL,
            CompatibilityLevel.FULL_TRANSITIVE,
        ):
            issues.extend(self._check_backward(new_fields, old_fields))
        
        if self.level in (
            CompatibilityLevel.FORWARD,
            CompatibilityLevel.FORWARD_TRANSITIVE,
            CompatibilityLevel.FULL,
            CompatibilityLevel.FULL_TRANSITIVE,
        ):
            issues.extend(self._check_forward(new_fields, old_fields))
        
        # Determine overall compatibility
        has_errors = any(i.severity == "error" for i in issues)
        
        return CompatibilityResult(
            is_compatible=not has_errors,
            level=self.level,
            issues=issues,
        )
    
    def _check_backward(
        self,
        new_fields: Dict[str, Dict],
        old_fields: Dict[str, Dict],
    ) -> List[CompatibilityIssue]:
        """
        Check backward compatibility (new reader, old writer).
        
        Rules:
        - New fields must have defaults
        - Cannot remove fields that old data has
        - Type changes must be compatible
        """
        issues = []
        
        # Check for new fields without defaults
        for name, field in new_fields.items():
            if name not in old_fields:
                if not self._has_default(field):
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="missing_default",
                        description="New field must have a default value for backward compatibility",
                        severity="error",
                    ))
        
        # Check for type changes in existing fields
        for name in new_fields.keys() & old_fields.keys():
            new_type = new_fields[name].get("type")
            old_type = old_fields[name].get("type")
            
            if not self._types_compatible(old_type, new_type):
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="type_change",
                    description=f"Type changed from {old_type} to {new_type}",
                    severity="error",
                ))
        
        return issues
    
    def _check_forward(
        self,
        new_fields: Dict[str, Dict],
        old_fields: Dict[str, Dict],
    ) -> List[CompatibilityIssue]:
        """
        Check forward compatibility (old reader, new writer).
        
        Rules:
        - Removed fields must have had defaults
        - Cannot add required fields
        - Type changes must be compatible
        """
        issues = []
        
        # Check for removed fields
        for name, field in old_fields.items():
            if name not in new_fields:
                if not self._has_default(field):
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="removed_required",
                        description="Cannot remove field without default for forward compatibility",
                        severity="error",
                    ))
        
        return issues
    
    def _has_default(self, field: Dict[str, Any]) -> bool:
        """Check if field has a default value."""
        # Explicit default
        if "default" in field:
            return True
        
        # Union with null first = implicit null default
        field_type = field.get("type")
        if isinstance(field_type, list) and len(field_type) > 0:
            if field_type[0] == "null":
                return True
        
        return False
    
    def _types_compatible(
        self,
        old_type: Any,
        new_type: Any,
    ) -> bool:
        """
        Check if types are compatible.
        
        Compatible type changes in Avro:
        - int → long, float, double
        - long → float, double
        - float → double
        - Adding null to union
        """
        # Same type = compatible
        if old_type == new_type:
            return True
        
        # Normalize to lists for union handling
        old_types = old_type if isinstance(old_type, list) else [old_type]
        new_types = new_type if isinstance(new_type, list) else [new_type]
        
        # New type is superset of old = compatible
        old_set = set(self._flatten_types(old_types))
        new_set = set(self._flatten_types(new_types))
        
        if old_set <= new_set:
            return True
        
        # Check numeric promotions
        numeric_promotions = {
            "int": {"long", "float", "double"},
            "long": {"float", "double"},
            "float": {"double"},
        }
        
        for old_t in old_set:
            for new_t in new_set:
                if old_t in numeric_promotions:
                    if new_t in numeric_promotions.get(old_t, set()):
                        return True
        
        return False
    
    def _flatten_types(self, types: List[Any]) -> List[str]:
        """Flatten complex types to simple names."""
        result = []
        for t in types:
            if isinstance(t, str):
                result.append(t)
            elif isinstance(t, dict):
                result.append(t.get("type", str(t)))
            else:
                result.append(str(t))
        return result


def check_backward_compatibility(
    new_schema: Dict[str, Any],
    old_schema: Dict[str, Any],
) -> CompatibilityResult:
    """
    Check if new schema can read data written by old schema.
    
    Args:
        new_schema: The new schema
        old_schema: The old schema
        
    Returns:
        CompatibilityResult
    """
    checker = CompatibilityChecker(CompatibilityLevel.BACKWARD)
    return checker.check(new_schema, old_schema)


def check_forward_compatibility(
    new_schema: Dict[str, Any],
    old_schema: Dict[str, Any],
) -> CompatibilityResult:
    """
    Check if old schema can read data written by new schema.
    
    Args:
        new_schema: The new schema
        old_schema: The old schema
        
    Returns:
        CompatibilityResult
    """
    checker = CompatibilityChecker(CompatibilityLevel.FORWARD)
    return checker.check(new_schema, old_schema)


def check_full_compatibility(
    new_schema: Dict[str, Any],
    old_schema: Dict[str, Any],
) -> CompatibilityResult:
    """
    Check if schemas are both backward and forward compatible.
    
    Args:
        new_schema: The new schema
        old_schema: The old schema
        
    Returns:
        CompatibilityResult
    """
    checker = CompatibilityChecker(CompatibilityLevel.FULL)
    return checker.check(new_schema, old_schema)


def get_schema_diff(
    new_schema: Dict[str, Any],
    old_schema: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Get detailed diff between two schemas.
    
    Args:
        new_schema: The new schema
        old_schema: The old schema
        
    Returns:
        Dictionary with added, removed, and modified fields
    """
    new_fields = {f["name"]: f for f in new_schema.get("fields", [])}
    old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
    
    added = set(new_fields.keys()) - set(old_fields.keys())
    removed = set(old_fields.keys()) - set(new_fields.keys())
    common = set(new_fields.keys()) & set(old_fields.keys())
    
    modified = {}
    for name in common:
        new_field = new_fields[name]
        old_field = old_fields[name]
        
        changes = {}
        
        if new_field.get("type") != old_field.get("type"):
            changes["type"] = {
                "old": old_field.get("type"),
                "new": new_field.get("type"),
            }
        
        if new_field.get("default") != old_field.get("default"):
            changes["default"] = {
                "old": old_field.get("default"),
                "new": new_field.get("default"),
            }
        
        if new_field.get("doc") != old_field.get("doc"):
            changes["doc"] = {
                "old": old_field.get("doc"),
                "new": new_field.get("doc"),
            }
        
        if changes:
            modified[name] = changes
    
    return {
        "added": list(added),
        "removed": list(removed),
        "modified": modified,
    }
