"""
Schema Evolution and Versioning Module

This module provides utilities for:
- Schema version management
- Compatibility checking (BACKWARD, FORWARD, FULL)
- Schema migration between versions
- Schema registry abstraction (local and remote)
"""

from .version import (
    SchemaVersion,
    VersionedSchema,
    parse_version,
    compare_versions,
)
from .compatibility import (
    CompatibilityLevel,
    CompatibilityChecker,
    CompatibilityResult,
    check_backward_compatibility,
    check_forward_compatibility,
    check_full_compatibility,
)
from .registry import (
    SchemaRegistry,
    LocalSchemaRegistry,
    SchemaMetadata,
)
from .migration import (
    SchemaMigrator,
    MigrationPlan,
    MigrationStep,
    FieldMapping,
)

__all__ = [
    # Version
    "SchemaVersion",
    "VersionedSchema",
    "parse_version",
    "compare_versions",
    # Compatibility
    "CompatibilityLevel",
    "CompatibilityChecker",
    "CompatibilityResult",
    "check_backward_compatibility",
    "check_forward_compatibility",
    "check_full_compatibility",
    # Registry
    "SchemaRegistry",
    "LocalSchemaRegistry",
    "SchemaMetadata",
    # Migration
    "SchemaMigrator",
    "MigrationPlan",
    "MigrationStep",
    "FieldMapping",
]
