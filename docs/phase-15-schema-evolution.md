# Phase 15: Schema Evolution & Versioning

## Overview

This phase implements a complete schema evolution system for the CDC pipeline. Schema evolution allows you to safely modify data schemas over time while maintaining backward/forward compatibility with existing producers and consumers.

## What We Built

### 1. Schema Versioning (`src/schemas/evolution/version.py`)

**Simple Explanation:**
Think of schema versions like software versions - they tell you what changed:
- **Major version** (1.0.0 → 2.0.0): Breaking changes - old readers can't read new data
- **Minor version** (1.0.0 → 1.1.0): New features - safe to upgrade
- **Patch version** (1.0.0 → 1.0.1): Bug fixes - no data changes

**Technical Details:**

```python
from src.schemas.evolution import SchemaVersion, VersionedSchema

# Create version
v1 = SchemaVersion(1, 0, 0)
print(v1)  # "1.0.0"

# Parse from string
v2 = SchemaVersion.parse("2.1.3")

# Version comparison
assert v1 < v2
assert v1.is_compatible_with(SchemaVersion(1, 5, 0))  # Same major = compatible

# Bump versions
v_next = v1.bump_minor()  # 1.1.0

# Wrap schema with version metadata
versioned = VersionedSchema(
    name="CDCEvent",
    version=v1,
    schema={"type": "record", "name": "CDCEvent", ...},
    description="Initial CDC event schema"
)

# Access schema properties
print(versioned.full_name)      # "CDCEvent-v1.0.0"
print(versioned.fingerprint)    # SHA256 hash for deduplication
print(versioned.field_names)    # ["id", "timestamp", "operation", ...]
```

### 2. Compatibility Checking (`src/schemas/evolution/compatibility.py`)

**Simple Explanation:**
Before changing a schema, we check if it's safe:
- **BACKWARD**: Can new code read old data? (Most common)
- **FORWARD**: Can old code read new data?
- **FULL**: Both directions safe

**Technical Details:**

```python
from src.schemas.evolution import (
    CompatibilityLevel,
    CompatibilityChecker,
    check_backward_compatibility,
    check_full_compatibility,
    get_schema_diff,
)

old_schema = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"},
    ]
}

# SAFE: Adding optional field (with default)
new_schema_safe = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"},
        {"name": "source", "type": ["null", "string"], "default": None},  # Optional!
    ]
}

# Check compatibility
result = check_backward_compatibility(new_schema_safe, old_schema)
print(result.is_compatible)  # True

# UNSAFE: Adding required field (no default)
new_schema_breaking = {
    "type": "record",
    "name": "Event", 
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"},
        {"name": "priority", "type": "int"},  # No default = BREAKING!
    ]
}

result = check_backward_compatibility(new_schema_breaking, old_schema)
print(result.is_compatible)  # False
print(result.summary())      # Shows error details

# Get detailed diff
diff = get_schema_diff(new_schema_safe, old_schema)
print(diff["added"])     # ["source"]
print(diff["removed"])   # []
print(diff["modified"])  # {}
```

**Compatibility Rules:**

| Change Type | BACKWARD | FORWARD | FULL |
|------------|----------|---------|------|
| Add optional field | ✅ | ✅ | ✅ |
| Add required field | ❌ | ✅ | ❌ |
| Remove optional field | ✅ | ❌ | ❌ |
| Remove required field | ✅ | ❌ | ❌ |
| Rename field | ❌ | ❌ | ❌ |
| int → long | ✅ | ❌ | ❌ |

### 3. Schema Registry (`src/schemas/evolution/registry.py`)

**Simple Explanation:**
A schema registry is like a library for schemas:
- Store schemas with unique IDs
- Track version history
- Enforce compatibility rules before registering

**Technical Details:**

```python
from src.schemas.evolution import LocalSchemaRegistry, CompatibilityLevel

# Initialize registry (stores in local files)
registry = LocalSchemaRegistry(
    registry_dir="./schema_registry",
    default_compatibility=CompatibilityLevel.BACKWARD
)

# Register a schema
schema_v1 = {"type": "record", "name": "Event", "fields": [...]}
schema_id = registry.register("cdc-event", schema_v1)
print(f"Registered with ID: {schema_id}")  # 1

# Register new version (compatibility checked automatically!)
schema_v2 = {...}  # Must be backward compatible
schema_id_v2 = registry.register("cdc-event", schema_v2)

# Get latest version
latest = registry.get_latest("cdc-event")
print(f"Version: {latest.version}")
print(f"Schema: {latest.schema}")

# Get specific version
v1 = registry.get_version("cdc-event", version=1)

# Get schema by ID (fast lookup)
schema = registry.get_schema(schema_id)

# List all subjects
subjects = registry.get_subjects()

# Check compatibility before registering
result = registry.check_compatibility("cdc-event", new_schema)
if result.is_compatible:
    registry.register("cdc-event", new_schema)
else:
    print(f"Cannot register: {result.summary()}")

# Change compatibility level per subject
registry.set_compatibility("cdc-event", CompatibilityLevel.FULL)
```

**Registry Storage Structure:**
```
schema_registry/
├── index.json           # Global ID mapping
├── schemas/
│   ├── 1.json          # Schema ID 1
│   ├── 2.json          # Schema ID 2
│   └── ...
└── subjects/
    ├── cdc-event.json  # Subject metadata
    └── ...
```

### 4. Schema Migration (`src/schemas/evolution/migration.py`)

**Simple Explanation:**
Migration transforms data from old schema format to new:
- Rename fields
- Add default values
- Transform data types
- Chain multiple migrations

**Technical Details:**

```python
from src.schemas.evolution import (
    SchemaMigrator,
    MigrationPlan,
    MigrationStep,
    MigrationType,
    create_field_addition_plan,
)

# Build a migration plan (fluent API)
v1_to_v2 = (
    MigrationPlan("1.0.0", "2.0.0")
    .add_field("source", default_value=None)
    .add_field("region", default_value="us-east-1")
    .rename_field("ts", "timestamp")
    .transform_field("data", lambda x: x.upper())
)

# Execute migration
old_record = {"id": "123", "ts": 1704067200, "data": "hello"}
new_record = v1_to_v2.execute(old_record)
# Result: {"id": "123", "timestamp": 1704067200, "data": "HELLO", 
#          "source": None, "region": "us-east-1"}

# Use migrator for multi-version chains
migrator = SchemaMigrator()

# Register migration paths
migrator.register_plan(v1_to_v2)
migrator.register_plan(
    MigrationPlan("2.0.0", "3.0.0")
    .remove_field("deprecated_field")
)

# Migrate across multiple versions (auto-finds path)
result = migrator.migrate(old_record, "1.0.0", "3.0.0")

# Batch migration
records = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
migrated = migrator.migrate_batch(records, "1.0.0", "2.0.0")

# Helper functions for common patterns
plan = create_field_addition_plan(
    "1.0.0", "2.0.0",
    {"source": None, "region": "default"},
    description="Add tracking fields"
)
```

**Built-in Transforms:**
```python
from src.schemas.evolution.migration import (
    string_to_int,       # "42" → 42
    int_to_string,       # 42 → "42"
    timestamp_ms_to_iso, # 1704067200000 → "2024-01-01T00:00:00+00:00"
    iso_to_timestamp_ms, # "2024-01-01T00:00:00Z" → 1704067200000
)
```

## Key Classes and Functions

### Version Module

| Class/Function | Purpose |
|---------------|---------|
| `SchemaVersion` | Semantic version representation |
| `VersionedSchema` | Schema + version metadata |
| `parse_version()` | Parse "1.2.3" to SchemaVersion |
| `compare_versions()` | Compare two versions |
| `detect_change_type()` | Detect if change is breaking |
| `suggest_version_bump()` | Suggest next version |

### Compatibility Module

| Class/Function | Purpose |
|---------------|---------|
| `CompatibilityLevel` | Enum: BACKWARD, FORWARD, FULL, NONE |
| `CompatibilityChecker` | Check schema compatibility |
| `CompatibilityResult` | Result with issues list |
| `check_backward_compatibility()` | Quick backward check |
| `check_full_compatibility()` | Quick full check |
| `get_schema_diff()` | Diff two schemas |

### Registry Module

| Class/Function | Purpose |
|---------------|---------|
| `SchemaRegistry` | Abstract base class |
| `LocalSchemaRegistry` | File-based registry |
| `SchemaMetadata` | Schema info (id, version, fingerprint) |

### Migration Module

| Class/Function | Purpose |
|---------------|---------|
| `SchemaMigrator` | Migrate data between versions |
| `MigrationPlan` | Ordered list of migration steps |
| `MigrationStep` | Single transformation |
| `FieldMapping` | Field source → target mapping |
| `create_field_addition_plan()` | Helper for adding fields |
| `create_field_removal_plan()` | Helper for removing fields |
| `create_field_rename_plan()` | Helper for renaming fields |

## Usage Patterns

### Pattern 1: Safe Schema Evolution

```python
# 1. Load current schema
registry = LocalSchemaRegistry("./schemas")
current = registry.get_latest("cdc-event")

# 2. Create new schema
new_schema = {
    ...current.schema,
    "fields": current.schema["fields"] + [
        {"name": "trace_id", "type": ["null", "string"], "default": None}
    ]
}

# 3. Check compatibility
result = registry.check_compatibility("cdc-event", new_schema)
if not result.is_compatible:
    print(f"Breaking change detected: {result.summary()}")
    raise ValueError("Cannot evolve schema")

# 4. Register new version
new_id = registry.register("cdc-event", new_schema)

# 5. Create migration for existing data
migrator = SchemaMigrator()
migrator.register_plan(
    MigrationPlan(str(current.version), "1.1.0")
    .add_field("trace_id", None)
)
```

### Pattern 2: Producer Schema Lookup

```python
class CDCProducer:
    def __init__(self, registry: SchemaRegistry):
        self.registry = registry
        self._schema_cache = {}
    
    def get_schema(self, subject: str) -> dict:
        if subject not in self._schema_cache:
            latest = self.registry.get_latest(subject)
            self._schema_cache[subject] = latest.schema
        return self._schema_cache[subject]
    
    def produce(self, event: dict, subject: str):
        schema = self.get_schema(subject)
        # Serialize with schema...
```

### Pattern 3: Consumer Schema Evolution

```python
class CDCConsumer:
    def __init__(self, registry: SchemaRegistry, migrator: SchemaMigrator):
        self.registry = registry
        self.migrator = migrator
        self.target_version = "2.0.0"
    
    def process(self, record: dict, schema_id: int):
        # Get schema version for this record
        schema = self.registry.get_schema(schema_id)
        record_version = schema.get("version", "1.0.0")
        
        # Migrate if needed
        if record_version != self.target_version:
            record = self.migrator.migrate(
                record, 
                record_version, 
                self.target_version
            )
        
        return record
```

## Interview Questions

### Basic Questions

**Q: What is schema evolution?**
A: Schema evolution is the ability to change a data schema over time while maintaining compatibility with existing data and applications. It's essential in distributed systems where producers and consumers may be updated independently.

**Q: What are the three main compatibility types?**
A: 
- **BACKWARD**: New schema can read old data (safe for consumers to upgrade first)
- **FORWARD**: Old schema can read new data (safe for producers to upgrade first)
- **FULL**: Both backward and forward compatible (safest, but most restrictive)

**Q: Why use semantic versioning for schemas?**
A: Semantic versioning (major.minor.patch) clearly communicates the nature of changes:
- Major: Breaking changes requiring migration
- Minor: New features, backward compatible
- Patch: Bug fixes, no functional change

### Intermediate Questions

**Q: What makes a schema change backward compatible?**
A: Changes that don't break existing readers:
- Adding fields WITH defaults
- Removing fields (old readers ignore them)
- Widening types (int → long)
- Adding values to enums

**Q: What makes a schema change breaking?**
A: Changes that break existing readers:
- Adding required fields without defaults
- Removing required fields
- Renaming fields (without aliases)
- Narrowing types (long → int)
- Changing field semantics

**Q: How does a schema registry help in production?**
A: A schema registry:
1. Centralizes schema storage
2. Enforces compatibility before deployment
3. Provides schema lookup by ID for efficiency
4. Tracks version history for debugging
5. Enables schema evolution policies

### Advanced Questions

**Q: How would you handle a breaking schema change in production?**
A: Several strategies:
1. **Dual write**: Write to both old and new topics during migration
2. **Consumer versioning**: Maintain consumers for multiple schema versions
3. **Event migration**: Backfill historical data with new schema
4. **Feature flags**: Gradually roll out new schema to subsets

**Q: How would you implement transitive compatibility checking?**
A: Transitive compatibility (BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE) requires checking against ALL previous versions, not just the latest. Implementation:
```python
def check_transitive(new_schema, subject):
    for version in registry.get_versions(subject):
        old = registry.get_version(subject, version)
        result = checker.check(new_schema, old.schema)
        if not result.is_compatible:
            return result
    return CompatibilityResult(is_compatible=True, ...)
```

**Q: How would you optimize schema migration for high-volume data?**
A: Optimizations:
1. **Lazy migration**: Migrate on read, not write
2. **Batch processing**: Migrate records in batches
3. **Caching**: Cache migration plans and compiled transforms
4. **Parallelization**: Migrate partitions in parallel
5. **Schema ID embedding**: Store schema ID with data to avoid lookups

## Test Results

```
tests/unit/test_schema_evolution.py: 60 tests passed

Test coverage:
- SchemaVersion: 6 tests
- VersionedSchema: 4 tests  
- ChangeTypeDetection: 3 tests
- CompatibilityChecker: 5 tests
- CompatibilityResult: 2 tests
- SchemaDiff: 2 tests
- LocalSchemaRegistry: 12 tests
- MigrationStep: 5 tests
- MigrationPlan: 4 tests
- SchemaMigrator: 7 tests
- MigrationHelpers: 3 tests
- TransformFunctions: 3 tests
- FieldMapping: 3 tests
```

## Files Created

| File | Purpose |
|------|---------|
| `src/schemas/evolution/__init__.py` | Module exports |
| `src/schemas/evolution/version.py` | Schema versioning |
| `src/schemas/evolution/compatibility.py` | Compatibility checking |
| `src/schemas/evolution/registry.py` | Local schema registry |
| `src/schemas/evolution/migration.py` | Data migration utilities |
| `tests/unit/test_schema_evolution.py` | Unit tests (60 tests) |
| `docs/phase-15-schema-evolution.md` | This documentation |

## Next Steps

- **Phase 16**: Multi-Region Support
- **Phase 17**: Security & Access Control
- **Phase 18**: Monitoring & Alerting
- **Phase 19**: CI/CD Pipeline
- **Phase 20**: Production Deployment

## Summary

Phase 15 provides a complete schema evolution system:

1. **Versioning**: Semantic versioning with comparison and bumping
2. **Compatibility**: BACKWARD, FORWARD, FULL checking per Avro rules
3. **Registry**: Local file-based registry with caching and deduplication
4. **Migration**: Fluent API for data transformation between versions

This enables safe schema changes in production without breaking existing producers or consumers.
