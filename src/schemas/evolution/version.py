"""
Schema Version Management

Provides semantic versioning for Avro schemas:
- SchemaVersion: Semantic version (major.minor.patch)
- VersionedSchema: Schema with version metadata
- Version comparison and parsing utilities
"""

import re
import json
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class SchemaVersion:
    """
    Semantic version for schemas.
    
    Format: major.minor.patch
    
    - major: Breaking changes (incompatible)
    - minor: New features (backward compatible)
    - patch: Bug fixes (backward compatible)
    
    SIMPLE EXPLANATION:
    Think of versions like recipe updates:
    - 1.0.0 → Original recipe
    - 1.0.1 → Fixed typo (patch)
    - 1.1.0 → Added optional ingredient (minor)
    - 2.0.0 → Changed cooking method (major)
    """
    
    major: int
    minor: int
    patch: int = 0
    
    def __post_init__(self):
        if self.major < 0 or self.minor < 0 or self.patch < 0:
            raise ValueError("Version components must be non-negative")
    
    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
    
    @classmethod
    def parse(cls, version_str: str) -> "SchemaVersion":
        """
        Parse version string.
        
        Args:
            version_str: Version like "1.2.3" or "v1.2.3"
            
        Returns:
            SchemaVersion instance
        """
        # Remove optional 'v' prefix
        version_str = version_str.lstrip("v")
        
        # Parse components
        match = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?$", version_str)
        if not match:
            raise ValueError(f"Invalid version format: {version_str}")
        
        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3)) if match.group(3) else 0
        
        return cls(major=major, minor=minor, patch=patch)
    
    def bump_major(self) -> "SchemaVersion":
        """Bump major version (breaking change)."""
        return SchemaVersion(major=self.major + 1, minor=0, patch=0)
    
    def bump_minor(self) -> "SchemaVersion":
        """Bump minor version (new feature)."""
        return SchemaVersion(major=self.major, minor=self.minor + 1, patch=0)
    
    def bump_patch(self) -> "SchemaVersion":
        """Bump patch version (bug fix)."""
        return SchemaVersion(major=self.major, minor=self.minor, patch=self.patch + 1)
    
    def is_compatible_with(self, other: "SchemaVersion") -> bool:
        """
        Check if this version is backward compatible with another.
        
        Backward compatible if major version matches.
        """
        return self.major == other.major
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return {
            "major": self.major,
            "minor": self.minor,
            "patch": self.patch,
        }


def parse_version(version_str: str) -> SchemaVersion:
    """Parse version string to SchemaVersion."""
    return SchemaVersion.parse(version_str)


def compare_versions(v1: SchemaVersion, v2: SchemaVersion) -> int:
    """
    Compare two versions.
    
    Returns:
        -1 if v1 < v2
        0 if v1 == v2
        1 if v1 > v2
    """
    if v1 < v2:
        return -1
    elif v1 > v2:
        return 1
    return 0


class ChangeType(Enum):
    """Type of schema change."""
    
    NONE = "none"                    # No change
    BACKWARD_COMPATIBLE = "backward" # Minor/patch change
    FORWARD_COMPATIBLE = "forward"   # Can read old data
    BREAKING = "breaking"            # Major change


@dataclass
class VersionedSchema:
    """
    Schema with version metadata.
    
    Combines an Avro schema with:
    - Version information
    - Fingerprint for deduplication
    - Creation timestamp
    - Optional description
    """
    
    name: str
    version: SchemaVersion
    schema: Dict[str, Any]
    fingerprint: str = field(default="")
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    description: str = ""
    previous_version: Optional[SchemaVersion] = None
    
    def __post_init__(self):
        if not self.fingerprint:
            self.fingerprint = self._compute_fingerprint()
    
    def _compute_fingerprint(self) -> str:
        """
        Compute schema fingerprint using canonical form.
        
        Fingerprint is a hash of the normalized schema, used for:
        - Deduplication
        - Cache keys
        - Change detection
        """
        # Canonical form: sorted keys, no whitespace
        canonical = json.dumps(self.schema, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]
    
    @property
    def full_name(self) -> str:
        """Full name with version: 'schema_name-v1.2.3'"""
        return f"{self.name}-v{self.version}"
    
    @property
    def namespace(self) -> Optional[str]:
        """Schema namespace from Avro schema."""
        return self.schema.get("namespace")
    
    @property
    def qualified_name(self) -> str:
        """Fully qualified name: 'namespace.name'"""
        if self.namespace:
            return f"{self.namespace}.{self.name}"
        return self.name
    
    @property
    def fields(self) -> List[Dict[str, Any]]:
        """Get schema fields (for record types)."""
        return self.schema.get("fields", [])
    
    @property
    def field_names(self) -> List[str]:
        """Get list of field names."""
        return [f["name"] for f in self.fields]
    
    def has_field(self, field_name: str) -> bool:
        """Check if schema has a field."""
        return field_name in self.field_names
    
    def get_field(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get field definition by name."""
        for field in self.fields:
            if field["name"] == field_name:
                return field
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to serializable dictionary."""
        return {
            "name": self.name,
            "version": str(self.version),
            "fingerprint": self.fingerprint,
            "created_at": self.created_at.isoformat(),
            "description": self.description,
            "previous_version": str(self.previous_version) if self.previous_version else None,
            "schema": self.schema,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionedSchema":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            version=SchemaVersion.parse(data["version"]),
            schema=data["schema"],
            fingerprint=data.get("fingerprint", ""),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(timezone.utc),
            description=data.get("description", ""),
            previous_version=SchemaVersion.parse(data["previous_version"]) if data.get("previous_version") else None,
        )
    
    @classmethod
    def from_file(
        cls,
        path: Path,
        name: Optional[str] = None,
        version: Optional[SchemaVersion] = None,
    ) -> "VersionedSchema":
        """
        Load schema from .avsc file.
        
        Args:
            path: Path to schema file
            name: Schema name (defaults to filename without extension)
            version: Schema version (defaults to 1.0.0)
            
        Returns:
            VersionedSchema instance
        """
        with open(path) as f:
            schema = json.load(f)
        
        # Extract name from schema or filename
        if name is None:
            name = schema.get("name", path.stem)
        
        # Default version
        if version is None:
            version = SchemaVersion(1, 0, 0)
        
        return cls(
            name=name,
            version=version,
            schema=schema,
            description=schema.get("doc", ""),
        )
    
    def save(self, path: Path) -> None:
        """Save schema to file."""
        with open(path, "w") as f:
            json.dump(self.schema, f, indent=2)
        logger.info(f"Saved schema {self.full_name} to {path}")


def detect_change_type(
    old_schema: VersionedSchema,
    new_schema: VersionedSchema,
) -> ChangeType:
    """
    Detect the type of change between two schema versions.
    
    Args:
        old_schema: Previous schema version
        new_schema: New schema version
        
    Returns:
        ChangeType indicating compatibility level
    """
    # Same fingerprint = no change
    if old_schema.fingerprint == new_schema.fingerprint:
        return ChangeType.NONE
    
    old_fields = set(old_schema.field_names)
    new_fields = set(new_schema.field_names)
    
    added_fields = new_fields - old_fields
    removed_fields = old_fields - new_fields
    
    # Fields removed = breaking change
    if removed_fields:
        logger.warning(f"Breaking change: removed fields {removed_fields}")
        return ChangeType.BREAKING
    
    # Check if added fields have defaults (backward compatible)
    if added_fields:
        for field_name in added_fields:
            field = new_schema.get_field(field_name)
            if field and "default" not in field:
                # Check if type is union with null (implicit default)
                field_type = field.get("type")
                if isinstance(field_type, list) and "null" in field_type:
                    continue  # Union with null has implicit default
                logger.warning(f"Breaking change: added field {field_name} without default")
                return ChangeType.BREAKING
        
        return ChangeType.BACKWARD_COMPATIBLE
    
    # Check for type changes in existing fields
    for field_name in old_fields & new_fields:
        old_field = old_schema.get_field(field_name)
        new_field = new_schema.get_field(field_name)
        
        if old_field and new_field:
            if old_field.get("type") != new_field.get("type"):
                # Type change could be breaking
                logger.warning(f"Potential breaking change: field {field_name} type changed")
                return ChangeType.BREAKING
    
    return ChangeType.BACKWARD_COMPATIBLE


def suggest_version_bump(
    current_version: SchemaVersion,
    change_type: ChangeType,
) -> SchemaVersion:
    """
    Suggest next version based on change type.
    
    Args:
        current_version: Current schema version
        change_type: Type of change detected
        
    Returns:
        Suggested new version
    """
    if change_type == ChangeType.NONE:
        return current_version
    elif change_type == ChangeType.BREAKING:
        return current_version.bump_major()
    elif change_type == ChangeType.BACKWARD_COMPATIBLE:
        return current_version.bump_minor()
    else:
        return current_version.bump_patch()
