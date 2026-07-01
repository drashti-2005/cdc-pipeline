"""
Unit tests for schema evolution module.

Tests:
- Schema versioning
- Compatibility checking
- Local schema registry
- Schema migration
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

from schemas.evolution.version import (
    SchemaVersion,
    VersionedSchema,
    ChangeType,
    parse_version,
    compare_versions,
    detect_change_type,
    suggest_version_bump,
)
from schemas.evolution.compatibility import (
    CompatibilityLevel,
    CompatibilityChecker,
    CompatibilityResult,
    CompatibilityIssue,
    check_backward_compatibility,
    check_forward_compatibility,
    check_full_compatibility,
    get_schema_diff,
)
from schemas.evolution.registry import (
    SchemaRegistry,
    LocalSchemaRegistry,
    SchemaMetadata,
)
from schemas.evolution.migration import (
    SchemaMigrator,
    MigrationPlan,
    MigrationStep,
    MigrationType,
    FieldMapping,
    create_field_addition_plan,
    create_field_removal_plan,
    create_field_rename_plan,
    string_to_int,
    int_to_string,
    timestamp_ms_to_iso,
    iso_to_timestamp_ms,
)


# ==============================================================================
# Test Fixtures
# ==============================================================================

@pytest.fixture
def sample_schema_v1():
    """Sample Avro schema v1."""
    return {
        "type": "record",
        "name": "CDCEvent",
        "namespace": "com.pipeline",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "operation", "type": "string"},
            {"name": "table_name", "type": "string"},
            {"name": "data", "type": {"type": "map", "values": "string"}},
        ]
    }


@pytest.fixture
def sample_schema_v2():
    """Sample Avro schema v2 - adds optional field."""
    return {
        "type": "record",
        "name": "CDCEvent",
        "namespace": "com.pipeline",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "operation", "type": "string"},
            {"name": "table_name", "type": "string"},
            {"name": "data", "type": {"type": "map", "values": "string"}},
            {"name": "source", "type": ["null", "string"], "default": None},
        ]
    }


@pytest.fixture
def sample_schema_v3():
    """Sample Avro schema v3 - adds required field (breaking)."""
    return {
        "type": "record",
        "name": "CDCEvent",
        "namespace": "com.pipeline",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "operation", "type": "string"},
            {"name": "table_name", "type": "string"},
            {"name": "data", "type": {"type": "map", "values": "string"}},
            {"name": "source", "type": ["null", "string"], "default": None},
            {"name": "partition_key", "type": "string"},  # No default = breaking!
        ]
    }


@pytest.fixture
def temp_registry_dir():
    """Create temp directory for registry tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


# ==============================================================================
# Schema Version Tests
# ==============================================================================

class TestSchemaVersion:
    """Tests for SchemaVersion class."""
    
    def test_version_creation(self):
        """Test version creation."""
        v = SchemaVersion(1, 2, 3)
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3
    
    def test_version_string(self):
        """Test version string representation."""
        v = SchemaVersion(1, 0, 0)
        assert str(v) == "1.0.0"
        
        v2 = SchemaVersion(2, 5, 10)
        assert str(v2) == "2.5.10"
    
    def test_version_comparison(self):
        """Test version comparison."""
        v1 = SchemaVersion(1, 0, 0)
        v2 = SchemaVersion(2, 0, 0)
        v1_1 = SchemaVersion(1, 1, 0)
        
        assert v1 < v2
        assert v1 < v1_1
        assert v2 > v1
        assert v1 == SchemaVersion(1, 0, 0)
    
    def test_parse_version(self):
        """Test parsing version strings."""
        v = parse_version("1.0.0")
        assert v.major == 1
        assert v.minor == 0
        assert v.patch == 0
        
        v2 = parse_version("2.5.10")
        assert v2.major == 2
        assert v2.minor == 5
        assert v2.patch == 10
    
    def test_compare_versions(self):
        """Test version comparison function."""
        v1 = parse_version("1.0.0")
        v2 = parse_version("2.0.0")
        v1_1 = parse_version("1.1.0")
        
        assert compare_versions(v1, v2) < 0
        assert compare_versions(v2, v1) > 0
        assert compare_versions(v1, v1) == 0
        assert compare_versions(v1_1, v1) > 0
    
    def test_version_bump(self):
        """Test version bumping."""
        v = SchemaVersion(1, 2, 3)
        
        v_major = v.bump_major()
        assert str(v_major) == "2.0.0"
        
        v_minor = v.bump_minor()
        assert str(v_minor) == "1.3.0"
        
        v_patch = v.bump_patch()
        assert str(v_patch) == "1.2.4"


class TestVersionedSchema:
    """Tests for VersionedSchema class."""
    
    def test_versioned_schema_creation(self, sample_schema_v1):
        """Test VersionedSchema creation."""
        version = SchemaVersion(1, 0, 0)
        vs = VersionedSchema(
            name="CDCEvent",
            version=version,
            schema=sample_schema_v1,
            description="Initial version",
        )
        
        assert vs.version == version
        assert vs.schema == sample_schema_v1
        assert vs.description == "Initial version"
        assert vs.name == "CDCEvent"
    
    def test_versioned_schema_to_dict(self, sample_schema_v1):
        """Test serialization to dict."""
        version = SchemaVersion(1, 0, 0)
        vs = VersionedSchema(
            name="CDCEvent",
            version=version,
            schema=sample_schema_v1,
        )
        
        d = vs.to_dict()
        assert d["version"] == "1.0.0"
        assert d["schema"] == sample_schema_v1
        assert d["name"] == "CDCEvent"
    
    def test_versioned_schema_full_name(self, sample_schema_v1):
        """Test full name property."""
        vs = VersionedSchema(
            name="CDCEvent",
            version=SchemaVersion(1, 2, 3),
            schema=sample_schema_v1,
        )
        
        assert vs.full_name == "CDCEvent-v1.2.3"
    
    def test_versioned_schema_field_access(self, sample_schema_v1):
        """Test field access methods."""
        vs = VersionedSchema(
            name="CDCEvent",
            version=SchemaVersion(1, 0, 0),
            schema=sample_schema_v1,
        )
        
        assert "id" in vs.field_names
        assert vs.has_field("id")
        assert not vs.has_field("nonexistent")
        
        id_field = vs.get_field("id")
        assert id_field is not None
        assert id_field["type"] == "string"


class TestChangeTypeDetection:
    """Tests for detecting change types."""
    
    def test_detect_field_addition(self, sample_schema_v1, sample_schema_v2):
        """Test detecting field additions."""
        vs1 = VersionedSchema(
            name="CDCEvent",
            version=SchemaVersion(1, 0, 0),
            schema=sample_schema_v1,
        )
        vs2 = VersionedSchema(
            name="CDCEvent",
            version=SchemaVersion(1, 1, 0),
            schema=sample_schema_v2,
        )
        
        change_type = detect_change_type(vs1, vs2)
        # Adding optional field (null union with default) is backward compatible
        assert change_type == ChangeType.BACKWARD_COMPATIBLE
    
    def test_detect_breaking_change(self, sample_schema_v2, sample_schema_v3):
        """Test detecting breaking changes."""
        vs2 = VersionedSchema(
            name="CDCEvent",
            version=SchemaVersion(1, 1, 0),
            schema=sample_schema_v2,
        )
        vs3 = VersionedSchema(
            name="CDCEvent",
            version=SchemaVersion(2, 0, 0),
            schema=sample_schema_v3,
        )
        
        change_type = detect_change_type(vs2, vs3)
        # Adding required field without default is breaking
        assert change_type == ChangeType.BREAKING
    
    def test_suggest_version_bump(self):
        """Test version bump suggestions."""
        current = SchemaVersion(1, 0, 0)
        
        # Backward compatible = minor bump
        suggested = suggest_version_bump(current, ChangeType.BACKWARD_COMPATIBLE)
        assert suggested.minor > current.minor
        
        # Breaking = major bump
        suggested_major = suggest_version_bump(current, ChangeType.BREAKING)
        assert suggested_major.major > current.major
        
        # No change = same version
        same = suggest_version_bump(current, ChangeType.NONE)
        assert same == current


# ==============================================================================
# Compatibility Tests
# ==============================================================================

class TestCompatibilityChecker:
    """Tests for CompatibilityChecker."""
    
    def test_backward_compatible_add_optional(self, sample_schema_v1, sample_schema_v2):
        """Test backward compatibility with optional field addition."""
        checker = CompatibilityChecker(CompatibilityLevel.BACKWARD)
        result = checker.check(sample_schema_v2, sample_schema_v1)
        
        assert result.is_compatible
        assert result.error_count == 0
    
    def test_backward_incompatible_add_required(self, sample_schema_v2, sample_schema_v3):
        """Test backward incompatibility with required field addition."""
        checker = CompatibilityChecker(CompatibilityLevel.BACKWARD)
        result = checker.check(sample_schema_v3, sample_schema_v2)
        
        assert not result.is_compatible
        assert result.error_count > 0
    
    def test_forward_compatible(self, sample_schema_v1, sample_schema_v2):
        """Test forward compatibility."""
        checker = CompatibilityChecker(CompatibilityLevel.FORWARD)
        result = checker.check(sample_schema_v2, sample_schema_v1)
        
        assert result.is_compatible
    
    def test_full_compatible(self, sample_schema_v1, sample_schema_v2):
        """Test full compatibility."""
        result = check_full_compatibility(sample_schema_v2, sample_schema_v1)
        assert result.is_compatible
    
    def test_compatibility_none_level(self, sample_schema_v1, sample_schema_v3):
        """Test NONE compatibility level allows anything."""
        checker = CompatibilityChecker(CompatibilityLevel.NONE)
        result = checker.check(sample_schema_v3, sample_schema_v1)
        
        assert result.is_compatible


class TestCompatibilityResult:
    """Tests for CompatibilityResult."""
    
    def test_result_summary(self):
        """Test result summary generation."""
        result = CompatibilityResult(
            is_compatible=False,
            level=CompatibilityLevel.BACKWARD,
            issues=[
                CompatibilityIssue(
                    field_name="new_field",
                    issue_type="missing_default",
                    description="Missing default value",
                    severity="error",
                )
            ],
        )
        
        summary = result.summary()
        assert "INCOMPATIBLE" in summary
        assert "new_field" in summary
    
    def test_error_warning_counts(self):
        """Test error and warning counting."""
        result = CompatibilityResult(
            is_compatible=False,
            level=CompatibilityLevel.FULL,
            issues=[
                CompatibilityIssue("f1", "t1", "d1", "error"),
                CompatibilityIssue("f2", "t2", "d2", "error"),
                CompatibilityIssue("f3", "t3", "d3", "warning"),
            ],
        )
        
        assert result.error_count == 2
        assert result.warning_count == 1


class TestSchemaDiff:
    """Tests for schema diff utility."""
    
    def test_get_schema_diff(self, sample_schema_v1, sample_schema_v2):
        """Test getting schema diff."""
        diff = get_schema_diff(sample_schema_v2, sample_schema_v1)
        
        assert "source" in diff["added"]
        assert len(diff["removed"]) == 0
    
    def test_diff_with_modifications(self):
        """Test diff with field modifications."""
        old_schema = {
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ]
        }
        new_schema = {
            "fields": [
                {"name": "id", "type": "long"},  # Type changed
                {"name": "name", "type": "string"},
            ]
        }
        
        diff = get_schema_diff(new_schema, old_schema)
        
        assert "id" in diff["modified"]
        assert diff["modified"]["id"]["type"]["old"] == "int"
        assert diff["modified"]["id"]["type"]["new"] == "long"


# ==============================================================================
# Local Schema Registry Tests
# ==============================================================================

class TestLocalSchemaRegistry:
    """Tests for LocalSchemaRegistry."""
    
    def test_registry_creation(self, temp_registry_dir):
        """Test registry initialization."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        assert Path(temp_registry_dir).exists()
        assert (Path(temp_registry_dir) / "schemas").exists()
        assert (Path(temp_registry_dir) / "subjects").exists()
    
    def test_register_schema(self, temp_registry_dir, sample_schema_v1):
        """Test registering a schema."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        schema_id = registry.register("cdc-event", sample_schema_v1)
        
        assert schema_id >= 1
        assert registry.get_schema(schema_id) == sample_schema_v1
    
    def test_get_latest(self, temp_registry_dir, sample_schema_v1, sample_schema_v2):
        """Test getting latest schema."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("cdc-event", sample_schema_v1)
        registry.register("cdc-event", sample_schema_v2)
        
        latest = registry.get_latest("cdc-event")
        
        assert latest is not None
        assert latest.version == 2
        assert latest.schema == sample_schema_v2
    
    def test_get_version(self, temp_registry_dir, sample_schema_v1, sample_schema_v2):
        """Test getting specific version."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("cdc-event", sample_schema_v1)
        registry.register("cdc-event", sample_schema_v2)
        
        v1 = registry.get_version("cdc-event", 1)
        v2 = registry.get_version("cdc-event", 2)
        
        assert v1.schema == sample_schema_v1
        assert v2.schema == sample_schema_v2
    
    def test_get_versions(self, temp_registry_dir, sample_schema_v1, sample_schema_v2):
        """Test getting all version numbers."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("cdc-event", sample_schema_v1)
        registry.register("cdc-event", sample_schema_v2)
        
        versions = registry.get_versions("cdc-event")
        
        assert versions == [1, 2]
    
    def test_get_subjects(self, temp_registry_dir, sample_schema_v1):
        """Test getting all subjects."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("subject-a", sample_schema_v1)
        registry.register("subject-b", sample_schema_v1)
        
        subjects = registry.get_subjects()
        
        assert "subject-a" in subjects
        assert "subject-b" in subjects
    
    def test_duplicate_schema_detection(self, temp_registry_dir, sample_schema_v1):
        """Test that duplicate schemas return existing ID."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        id1 = registry.register("cdc-event", sample_schema_v1)
        id2 = registry.register("cdc-event", sample_schema_v1)
        
        assert id1 == id2
    
    def test_compatibility_enforcement(self, temp_registry_dir, sample_schema_v1, sample_schema_v3):
        """Test that incompatible schemas are rejected."""
        registry = LocalSchemaRegistry(
            temp_registry_dir,
            default_compatibility=CompatibilityLevel.BACKWARD,
        )
        
        registry.register("cdc-event", sample_schema_v1)
        
        # v3 adds required field without default - should fail
        with pytest.raises(ValueError) as exc_info:
            registry.register("cdc-event", sample_schema_v3)
        
        assert "not compatible" in str(exc_info.value).lower()
    
    def test_set_compatibility(self, temp_registry_dir, sample_schema_v1):
        """Test setting compatibility level."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("cdc-event", sample_schema_v1)
        registry.set_compatibility("cdc-event", CompatibilityLevel.NONE)
        
        level = registry.get_compatibility("cdc-event")
        assert level == CompatibilityLevel.NONE
    
    def test_check_compatibility(self, temp_registry_dir, sample_schema_v1, sample_schema_v2):
        """Test compatibility checking."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("cdc-event", sample_schema_v1)
        
        result = registry.check_compatibility("cdc-event", sample_schema_v2)
        
        assert result.is_compatible
    
    def test_delete_subject(self, temp_registry_dir, sample_schema_v1):
        """Test deleting a subject."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        registry.register("cdc-event", sample_schema_v1)
        
        deleted = registry.delete_subject("cdc-event")
        
        assert deleted
        assert registry.get_latest("cdc-event") is None
    
    def test_schema_metadata(self, temp_registry_dir, sample_schema_v1):
        """Test SchemaMetadata structure."""
        registry = LocalSchemaRegistry(temp_registry_dir)
        
        schema_id = registry.register("cdc-event", sample_schema_v1)
        metadata = registry.get_latest("cdc-event")
        
        assert metadata.schema_id == schema_id
        assert metadata.subject == "cdc-event"
        assert metadata.version == 1
        assert metadata.fingerprint  # Should have a fingerprint
        assert isinstance(metadata.registered_at, datetime)


# ==============================================================================
# Migration Tests
# ==============================================================================

class TestMigrationStep:
    """Tests for MigrationStep."""
    
    def test_add_field_step(self):
        """Test adding a field."""
        step = MigrationStep(
            migration_type=MigrationType.ADD_FIELD,
            field_name="new_field",
            default_value="default",
        )
        
        data = {"existing": "value"}
        result = step.apply(data)
        
        assert result["new_field"] == "default"
        assert result["existing"] == "value"
    
    def test_remove_field_step(self):
        """Test removing a field."""
        step = MigrationStep(
            migration_type=MigrationType.REMOVE_FIELD,
            field_name="to_remove",
        )
        
        data = {"to_remove": "value", "keep": "value"}
        result = step.apply(data)
        
        assert "to_remove" not in result
        assert result["keep"] == "value"
    
    def test_rename_field_step(self):
        """Test renaming a field."""
        step = MigrationStep(
            migration_type=MigrationType.RENAME_FIELD,
            field_name="old_name",
            new_field_name="new_name",
        )
        
        data = {"old_name": "value"}
        result = step.apply(data)
        
        assert "old_name" not in result
        assert result["new_name"] == "value"
    
    def test_transform_field_step(self):
        """Test transforming a field."""
        step = MigrationStep(
            migration_type=MigrationType.TRANSFORM,
            field_name="count",
            transform=lambda x: x * 2,
        )
        
        data = {"count": 5}
        result = step.apply(data)
        
        assert result["count"] == 10
    
    def test_copy_field_step(self):
        """Test copying a field."""
        step = MigrationStep(
            migration_type=MigrationType.COPY_FIELD,
            field_name="original",
            new_field_name="copy",
        )
        
        data = {"original": "value"}
        result = step.apply(data)
        
        assert result["original"] == "value"
        assert result["copy"] == "value"


class TestMigrationPlan:
    """Tests for MigrationPlan."""
    
    def test_plan_creation(self):
        """Test creating a migration plan."""
        plan = MigrationPlan(
            source_version="1.0.0",
            target_version="2.0.0",
            description="Add source field",
        )
        
        assert plan.source_version == "1.0.0"
        assert plan.target_version == "2.0.0"
    
    def test_fluent_api(self):
        """Test fluent API for building plans."""
        plan = (
            MigrationPlan("1.0.0", "2.0.0")
            .add_field("new_field", "default")
            .rename_field("old", "new")
            .remove_field("deprecated")
        )
        
        assert len(plan.steps) == 3
    
    def test_plan_execution(self):
        """Test executing a migration plan."""
        plan = (
            MigrationPlan("1.0.0", "2.0.0")
            .add_field("version", "2.0.0")
            .rename_field("ts", "timestamp")
        )
        
        data = {"id": "123", "ts": 1234567890}
        result = plan.execute(data)
        
        assert result["version"] == "2.0.0"
        assert result["timestamp"] == 1234567890
        assert "ts" not in result
    
    def test_plan_summary(self):
        """Test plan summary generation."""
        plan = (
            MigrationPlan("1.0.0", "2.0.0")
            .add_field("source", None)
        )
        
        summary = plan.summary()
        
        assert "1.0.0" in summary
        assert "2.0.0" in summary
        assert "Add" in summary


class TestSchemaMigrator:
    """Tests for SchemaMigrator."""
    
    def test_register_and_get_plan(self):
        """Test registering and retrieving plans."""
        migrator = SchemaMigrator()
        
        plan = MigrationPlan("1.0.0", "2.0.0")
        migrator.register_plan(plan)
        
        retrieved = migrator.get_plan("1.0.0", "2.0.0")
        
        assert retrieved == plan
    
    def test_direct_migration(self):
        """Test direct migration between versions."""
        migrator = SchemaMigrator()
        
        plan = MigrationPlan("1.0.0", "2.0.0").add_field("version", "2")
        migrator.register_plan(plan)
        
        data = {"id": "123"}
        result = migrator.migrate(data, "1.0.0", "2.0.0")
        
        assert result["version"] == "2"
    
    def test_multi_step_migration(self):
        """Test migration through multiple versions."""
        migrator = SchemaMigrator()
        
        # v1 → v2: Add field
        migrator.register_plan(
            MigrationPlan("1.0.0", "2.0.0").add_field("source", None)
        )
        
        # v2 → v3: Rename field
        migrator.register_plan(
            MigrationPlan("2.0.0", "3.0.0").rename_field("source", "origin")
        )
        
        data = {"id": "123"}
        result = migrator.migrate(data, "1.0.0", "3.0.0")
        
        assert "source" not in result
        assert result["origin"] is None
    
    def test_find_migration_path(self):
        """Test finding migration path."""
        migrator = SchemaMigrator()
        
        migrator.register_plan(MigrationPlan("1.0.0", "2.0.0"))
        migrator.register_plan(MigrationPlan("2.0.0", "3.0.0"))
        migrator.register_plan(MigrationPlan("3.0.0", "4.0.0"))
        
        path = migrator.find_migration_path("1.0.0", "4.0.0")
        
        assert len(path) == 3
        assert path[0].source_version == "1.0.0"
        assert path[-1].target_version == "4.0.0"
    
    def test_no_path_raises(self):
        """Test that missing path raises error."""
        migrator = SchemaMigrator()
        
        migrator.register_plan(MigrationPlan("1.0.0", "2.0.0"))
        # No path to 5.0.0
        
        with pytest.raises(ValueError) as exc_info:
            migrator.migrate({}, "1.0.0", "5.0.0")
        
        assert "no migration path" in str(exc_info.value).lower()
    
    def test_same_version_returns_copy(self):
        """Test migrating to same version returns copy."""
        migrator = SchemaMigrator()
        
        data = {"id": "123"}
        result = migrator.migrate(data, "1.0.0", "1.0.0")
        
        assert result == data
        assert result is not data  # Should be a copy
    
    def test_batch_migration(self):
        """Test migrating multiple records."""
        migrator = SchemaMigrator()
        
        migrator.register_plan(
            MigrationPlan("1.0.0", "2.0.0").add_field("migrated", True)
        )
        
        records = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        results = migrator.migrate_batch(records, "1.0.0", "2.0.0")
        
        assert len(results) == 3
        assert all(r["migrated"] for r in results)
    
    def test_registered_versions(self):
        """Test getting registered versions."""
        migrator = SchemaMigrator()
        
        migrator.register_plan(MigrationPlan("1.0.0", "2.0.0"))
        migrator.register_plan(MigrationPlan("2.0.0", "3.0.0"))
        
        versions = migrator.registered_versions
        
        assert "1.0.0" in versions
        assert "2.0.0" in versions
        assert "3.0.0" in versions


class TestMigrationHelpers:
    """Tests for migration helper functions."""
    
    def test_create_field_addition_plan(self):
        """Test creating field addition plan."""
        plan = create_field_addition_plan(
            "1.0.0", "2.0.0",
            {"source": None, "region": "us-east-1"},
        )
        
        data = {}
        result = plan.execute(data)
        
        assert result["source"] is None
        assert result["region"] == "us-east-1"
    
    def test_create_field_removal_plan(self):
        """Test creating field removal plan."""
        plan = create_field_removal_plan(
            "1.0.0", "2.0.0",
            ["deprecated", "legacy"],
        )
        
        data = {"keep": "v", "deprecated": "x", "legacy": "y"}
        result = plan.execute(data)
        
        assert result["keep"] == "v"
        assert "deprecated" not in result
        assert "legacy" not in result
    
    def test_create_field_rename_plan(self):
        """Test creating field rename plan."""
        plan = create_field_rename_plan(
            "1.0.0", "2.0.0",
            {"ts": "timestamp", "op": "operation"},
        )
        
        data = {"ts": 123, "op": "INSERT"}
        result = plan.execute(data)
        
        assert result["timestamp"] == 123
        assert result["operation"] == "INSERT"


class TestTransformFunctions:
    """Tests for built-in transform functions."""
    
    def test_string_to_int(self):
        """Test string to int conversion."""
        assert string_to_int("42") == 42
        assert string_to_int(None) == 0
    
    def test_int_to_string(self):
        """Test int to string conversion."""
        assert int_to_string(42) == "42"
        assert int_to_string(None) == ""
    
    def test_timestamp_conversions(self):
        """Test timestamp conversions."""
        ts_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        
        iso_str = timestamp_ms_to_iso(ts_ms)
        assert "2024-01-01" in iso_str
        
        back_to_ts = iso_to_timestamp_ms(iso_str)
        assert back_to_ts == ts_ms


class TestFieldMapping:
    """Tests for FieldMapping."""
    
    def test_basic_mapping(self):
        """Test basic field mapping."""
        mapping = FieldMapping(
            source_field="old_name",
            target_field="new_name",
        )
        
        result = mapping.apply("value")
        assert result == "value"
    
    def test_mapping_with_transform(self):
        """Test mapping with transformation."""
        mapping = FieldMapping(
            source_field="count",
            target_field="count_doubled",
            transform=lambda x: x * 2,
        )
        
        result = mapping.apply(5)
        assert result == 10
    
    def test_mapping_with_default(self):
        """Test mapping with default value."""
        mapping = FieldMapping(
            source_field="optional",
            target_field="optional",
            default_value="default",
        )
        
        result = mapping.apply(None)
        assert result == "default"
