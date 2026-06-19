"""
Schema Migration Utilities

Provides tools for migrating data between schema versions:
- FieldMapping: Define how fields map between versions
- MigrationStep: Single transformation step
- MigrationPlan: Complete migration path
- SchemaMigrator: Execute migrations

SIMPLE EXPLANATION:
Schema migration is like translating between languages:
- Old schema: "color" field
- New schema: "colour" field (renamed)
- Migration: Convert "color" → "colour"
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable, Union
from enum import Enum
from copy import deepcopy

logger = logging.getLogger(__name__)


class MigrationType(Enum):
    """Type of migration operation."""
    
    ADD_FIELD = "add_field"           # Add new field with default
    REMOVE_FIELD = "remove_field"     # Remove field
    RENAME_FIELD = "rename_field"     # Rename field
    CHANGE_TYPE = "change_type"       # Change field type
    TRANSFORM = "transform"           # Apply transformation function
    COPY_FIELD = "copy_field"         # Copy field to new name


@dataclass
class FieldMapping:
    """
    Mapping between fields in different schema versions.
    
    Defines how to transform a field from old to new schema.
    """
    
    source_field: str
    target_field: str
    transform: Optional[Callable[[Any], Any]] = None
    default_value: Any = None
    
    def apply(self, value: Any) -> Any:
        """Apply transformation to value."""
        if value is None and self.default_value is not None:
            return self.default_value
        
        if self.transform:
            return self.transform(value)
        
        return value


@dataclass
class MigrationStep:
    """
    A single migration step.
    
    Represents one transformation to apply during migration.
    """
    
    migration_type: MigrationType
    field_name: str
    new_field_name: Optional[str] = None
    default_value: Any = None
    transform: Optional[Callable[[Any], Any]] = None
    description: str = ""
    
    def apply(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply migration step to data.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Transformed data dictionary
        """
        result = deepcopy(data)
        
        if self.migration_type == MigrationType.ADD_FIELD:
            if self.field_name not in result:
                result[self.field_name] = self.default_value
                logger.debug(f"Added field {self.field_name} = {self.default_value}")
        
        elif self.migration_type == MigrationType.REMOVE_FIELD:
            if self.field_name in result:
                del result[self.field_name]
                logger.debug(f"Removed field {self.field_name}")
        
        elif self.migration_type == MigrationType.RENAME_FIELD:
            if self.field_name in result and self.new_field_name:
                result[self.new_field_name] = result.pop(self.field_name)
                logger.debug(f"Renamed {self.field_name} → {self.new_field_name}")
        
        elif self.migration_type == MigrationType.COPY_FIELD:
            if self.field_name in result and self.new_field_name:
                result[self.new_field_name] = deepcopy(result[self.field_name])
                logger.debug(f"Copied {self.field_name} → {self.new_field_name}")
        
        elif self.migration_type == MigrationType.CHANGE_TYPE:
            if self.field_name in result and self.transform:
                result[self.field_name] = self.transform(result[self.field_name])
                logger.debug(f"Transformed type of {self.field_name}")
        
        elif self.migration_type == MigrationType.TRANSFORM:
            if self.field_name in result and self.transform:
                result[self.field_name] = self.transform(result[self.field_name])
                logger.debug(f"Applied transform to {self.field_name}")
        
        return result
    
    def __str__(self) -> str:
        if self.migration_type == MigrationType.RENAME_FIELD:
            return f"Rename {self.field_name} → {self.new_field_name}"
        elif self.migration_type == MigrationType.ADD_FIELD:
            return f"Add {self.field_name} = {self.default_value}"
        elif self.migration_type == MigrationType.REMOVE_FIELD:
            return f"Remove {self.field_name}"
        else:
            return f"{self.migration_type.value}: {self.field_name}"


@dataclass
class MigrationPlan:
    """
    A complete migration plan from one schema version to another.
    
    Contains an ordered list of migration steps to execute.
    """
    
    source_version: str
    target_version: str
    steps: List[MigrationStep] = field(default_factory=list)
    description: str = ""
    
    def add_step(self, step: MigrationStep) -> "MigrationPlan":
        """Add a migration step."""
        self.steps.append(step)
        return self
    
    def add_field(
        self,
        field_name: str,
        default_value: Any = None,
        description: str = "",
    ) -> "MigrationPlan":
        """Add a new field with default value."""
        self.steps.append(MigrationStep(
            migration_type=MigrationType.ADD_FIELD,
            field_name=field_name,
            default_value=default_value,
            description=description,
        ))
        return self
    
    def remove_field(
        self,
        field_name: str,
        description: str = "",
    ) -> "MigrationPlan":
        """Remove a field."""
        self.steps.append(MigrationStep(
            migration_type=MigrationType.REMOVE_FIELD,
            field_name=field_name,
            description=description,
        ))
        return self
    
    def rename_field(
        self,
        old_name: str,
        new_name: str,
        description: str = "",
    ) -> "MigrationPlan":
        """Rename a field."""
        self.steps.append(MigrationStep(
            migration_type=MigrationType.RENAME_FIELD,
            field_name=old_name,
            new_field_name=new_name,
            description=description,
        ))
        return self
    
    def transform_field(
        self,
        field_name: str,
        transform: Callable[[Any], Any],
        description: str = "",
    ) -> "MigrationPlan":
        """Apply transformation to a field."""
        self.steps.append(MigrationStep(
            migration_type=MigrationType.TRANSFORM,
            field_name=field_name,
            transform=transform,
            description=description,
        ))
        return self
    
    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute migration plan on data.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Migrated data dictionary
        """
        result = deepcopy(data)
        
        for step in self.steps:
            result = step.apply(result)
        
        return result
    
    def summary(self) -> str:
        """Generate human-readable summary."""
        lines = [
            f"Migration Plan: {self.source_version} → {self.target_version}",
            f"Steps: {len(self.steps)}",
        ]
        
        if self.description:
            lines.append(f"Description: {self.description}")
        
        lines.append("")
        for i, step in enumerate(self.steps, 1):
            lines.append(f"  {i}. {step}")
        
        return "\n".join(lines)


class SchemaMigrator:
    """
    Migrate data between schema versions.
    
    Supports:
    - Single-step migrations (v1 → v2)
    - Multi-step migrations (v1 → v2 → v3)
    - Automatic migration path finding
    
    USAGE:
        migrator = SchemaMigrator()
        
        # Register migration plans
        migrator.register_plan(v1_to_v2_plan)
        migrator.register_plan(v2_to_v3_plan)
        
        # Migrate data
        new_data = migrator.migrate(old_data, "1.0.0", "3.0.0")
    """
    
    def __init__(self):
        self._plans: Dict[tuple, MigrationPlan] = {}
    
    def register_plan(self, plan: MigrationPlan) -> None:
        """Register a migration plan."""
        key = (plan.source_version, plan.target_version)
        self._plans[key] = plan
        logger.info(f"Registered migration: {plan.source_version} → {plan.target_version}")
    
    def get_plan(
        self,
        source_version: str,
        target_version: str,
    ) -> Optional[MigrationPlan]:
        """Get direct migration plan between versions."""
        return self._plans.get((source_version, target_version))
    
    def find_migration_path(
        self,
        source_version: str,
        target_version: str,
    ) -> Optional[List[MigrationPlan]]:
        """
        Find migration path between versions.
        
        Uses BFS to find shortest path through registered migrations.
        
        Args:
            source_version: Starting version
            target_version: Target version
            
        Returns:
            List of migration plans to execute, or None if no path
        """
        if source_version == target_version:
            return []
        
        # Direct path?
        direct = self.get_plan(source_version, target_version)
        if direct:
            return [direct]
        
        # BFS for path
        from collections import deque
        
        queue = deque([(source_version, [])])
        visited = {source_version}
        
        while queue:
            current, path = queue.popleft()
            
            # Find all migrations from current version
            for (src, tgt), plan in self._plans.items():
                if src == current and tgt not in visited:
                    new_path = path + [plan]
                    
                    if tgt == target_version:
                        return new_path
                    
                    visited.add(tgt)
                    queue.append((tgt, new_path))
        
        return None
    
    def migrate(
        self,
        data: Dict[str, Any],
        source_version: str,
        target_version: str,
    ) -> Dict[str, Any]:
        """
        Migrate data from source to target version.
        
        Args:
            data: Input data dictionary
            source_version: Current version of data
            target_version: Target version
            
        Returns:
            Migrated data dictionary
            
        Raises:
            ValueError: If no migration path exists
        """
        if source_version == target_version:
            return deepcopy(data)
        
        path = self.find_migration_path(source_version, target_version)
        
        if path is None:
            raise ValueError(
                f"No migration path from {source_version} to {target_version}"
            )
        
        result = deepcopy(data)
        
        for plan in path:
            logger.info(f"Applying migration: {plan.source_version} → {plan.target_version}")
            result = plan.execute(result)
        
        return result
    
    def migrate_batch(
        self,
        records: List[Dict[str, Any]],
        source_version: str,
        target_version: str,
    ) -> List[Dict[str, Any]]:
        """
        Migrate a batch of records.
        
        Args:
            records: List of data dictionaries
            source_version: Current version
            target_version: Target version
            
        Returns:
            List of migrated records
        """
        path = self.find_migration_path(source_version, target_version)
        
        if path is None:
            raise ValueError(
                f"No migration path from {source_version} to {target_version}"
            )
        
        results = []
        for record in records:
            result = deepcopy(record)
            for plan in path:
                result = plan.execute(result)
            results.append(result)
        
        return results
    
    @property
    def registered_versions(self) -> set:
        """Get all registered versions."""
        versions = set()
        for src, tgt in self._plans.keys():
            versions.add(src)
            versions.add(tgt)
        return versions
    
    def list_plans(self) -> List[str]:
        """List all registered migration plans."""
        return [f"{src} → {tgt}" for src, tgt in self._plans.keys()]


# Common transformation functions
def string_to_int(value: Any) -> int:
    """Convert string to integer."""
    if value is None:
        return 0
    return int(value)


def int_to_string(value: Any) -> str:
    """Convert integer to string."""
    if value is None:
        return ""
    return str(value)


def timestamp_ms_to_iso(value: int) -> str:
    """Convert Unix timestamp (ms) to ISO string."""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return dt.isoformat()


def iso_to_timestamp_ms(value: str) -> int:
    """Convert ISO string to Unix timestamp (ms)."""
    from datetime import datetime
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def json_to_dict(value: str) -> Dict[str, Any]:
    """Parse JSON string to dictionary."""
    import json
    if value is None:
        return {}
    return json.loads(value)


def dict_to_json(value: Dict[str, Any]) -> str:
    """Convert dictionary to JSON string."""
    import json
    if value is None:
        return "{}"
    return json.dumps(value)


# Pre-built migration helpers
def create_field_addition_plan(
    source_version: str,
    target_version: str,
    new_fields: Dict[str, Any],
    description: str = "",
) -> MigrationPlan:
    """
    Create a migration plan that adds new fields.
    
    Args:
        source_version: Source version
        target_version: Target version
        new_fields: Dict of field_name → default_value
        description: Plan description
        
    Returns:
        MigrationPlan
    """
    plan = MigrationPlan(
        source_version=source_version,
        target_version=target_version,
        description=description or f"Add fields: {list(new_fields.keys())}",
    )
    
    for field_name, default_value in new_fields.items():
        plan.add_field(field_name, default_value)
    
    return plan


def create_field_removal_plan(
    source_version: str,
    target_version: str,
    removed_fields: List[str],
    description: str = "",
) -> MigrationPlan:
    """
    Create a migration plan that removes fields.
    
    Args:
        source_version: Source version
        target_version: Target version
        removed_fields: List of field names to remove
        description: Plan description
        
    Returns:
        MigrationPlan
    """
    plan = MigrationPlan(
        source_version=source_version,
        target_version=target_version,
        description=description or f"Remove fields: {removed_fields}",
    )
    
    for field_name in removed_fields:
        plan.remove_field(field_name)
    
    return plan


def create_field_rename_plan(
    source_version: str,
    target_version: str,
    renames: Dict[str, str],
    description: str = "",
) -> MigrationPlan:
    """
    Create a migration plan that renames fields.
    
    Args:
        source_version: Source version
        target_version: Target version
        renames: Dict of old_name → new_name
        description: Plan description
        
    Returns:
        MigrationPlan
    """
    plan = MigrationPlan(
        source_version=source_version,
        target_version=target_version,
        description=description or f"Rename fields: {list(renames.keys())}",
    )
    
    for old_name, new_name in renames.items():
        plan.rename_field(old_name, new_name)
    
    return plan
