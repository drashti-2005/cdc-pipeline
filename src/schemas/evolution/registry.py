"""
Schema Registry Abstraction

Provides a unified interface for schema storage and retrieval:
- LocalSchemaRegistry: File-based storage
- Future: ConfluentSchemaRegistry, KarapaceSchemaRegistry

SIMPLE EXPLANATION:
A schema registry is like a library catalog:
- Each schema has a unique ID
- You can look up schemas by name or ID
- New versions are tracked over time
- Compatibility is enforced before registration
"""

import json
import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

from src.schemas.evolution.version import SchemaVersion, VersionedSchema
from src.schemas.evolution.compatibility import (
    CompatibilityLevel,
    CompatibilityChecker,
    CompatibilityResult,
)

logger = logging.getLogger(__name__)


@dataclass
class SchemaMetadata:
    """Metadata about a registered schema."""
    
    schema_id: int
    subject: str
    version: int
    schema: Dict[str, Any]
    fingerprint: str
    registered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.schema_id,
            "subject": self.subject,
            "version": self.version,
            "schema": self.schema,
            "fingerprint": self.fingerprint,
            "registered_at": self.registered_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaMetadata":
        """Create from dictionary."""
        return cls(
            schema_id=data["id"],
            subject=data["subject"],
            version=data["version"],
            schema=data["schema"],
            fingerprint=data["fingerprint"],
            registered_at=datetime.fromisoformat(data["registered_at"]) if "registered_at" in data else datetime.now(timezone.utc),
        )


class SchemaRegistry(ABC):
    """
    Abstract base class for schema registries.
    
    A schema registry provides:
    - Schema storage by subject (name)
    - Version management
    - Compatibility checking
    - Schema lookup by ID
    """
    
    @abstractmethod
    def register(
        self,
        subject: str,
        schema: Dict[str, Any],
    ) -> int:
        """
        Register a new schema version.
        
        Args:
            subject: Schema subject name
            schema: Avro schema dictionary
            
        Returns:
            Schema ID
            
        Raises:
            ValueError: If schema is not compatible
        """
        pass
    
    @abstractmethod
    def get_schema(self, schema_id: int) -> Optional[Dict[str, Any]]:
        """Get schema by ID."""
        pass
    
    @abstractmethod
    def get_latest(self, subject: str) -> Optional[SchemaMetadata]:
        """Get latest schema version for subject."""
        pass
    
    @abstractmethod
    def get_version(self, subject: str, version: int) -> Optional[SchemaMetadata]:
        """Get specific version of a schema."""
        pass
    
    @abstractmethod
    def get_versions(self, subject: str) -> List[int]:
        """Get all version numbers for a subject."""
        pass
    
    @abstractmethod
    def get_subjects(self) -> List[str]:
        """Get all registered subjects."""
        pass
    
    @abstractmethod
    def check_compatibility(
        self,
        subject: str,
        schema: Dict[str, Any],
    ) -> CompatibilityResult:
        """Check if schema is compatible with latest version."""
        pass
    
    @abstractmethod
    def set_compatibility(
        self,
        subject: str,
        level: CompatibilityLevel,
    ) -> None:
        """Set compatibility level for a subject."""
        pass
    
    @abstractmethod
    def get_compatibility(self, subject: str) -> CompatibilityLevel:
        """Get compatibility level for a subject."""
        pass


class LocalSchemaRegistry(SchemaRegistry):
    """
    File-based schema registry for local development.
    
    Stores schemas in JSON files on disk:
    - schemas/: Schema definitions
    - subjects/: Subject metadata (versions, compatibility)
    - index.json: Global ID mapping
    
    USAGE:
        registry = LocalSchemaRegistry("./schema_registry")
        
        # Register a schema
        schema_id = registry.register("cdc-event", schema_dict)
        
        # Get latest version
        latest = registry.get_latest("cdc-event")
        
        # Check compatibility before update
        result = registry.check_compatibility("cdc-event", new_schema)
        if result.is_compatible:
            registry.register("cdc-event", new_schema)
    """
    
    def __init__(
        self,
        registry_dir: str = "./schema_registry",
        default_compatibility: CompatibilityLevel = CompatibilityLevel.BACKWARD,
    ):
        """
        Initialize local registry.
        
        Args:
            registry_dir: Directory to store registry data
            default_compatibility: Default compatibility level
        """
        self.registry_dir = Path(registry_dir)
        self.default_compatibility = default_compatibility
        
        # Create directories
        self.schemas_dir = self.registry_dir / "schemas"
        self.subjects_dir = self.registry_dir / "subjects"
        self.schemas_dir.mkdir(parents=True, exist_ok=True)
        self.subjects_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory caches
        self._schema_cache: Dict[int, Dict[str, Any]] = {}
        self._subject_cache: Dict[str, Dict] = {}
        self._next_id: int = 1
        self._lock = threading.Lock()
        
        # Load existing data
        self._load_index()
        
        logger.info(f"LocalSchemaRegistry initialized at {self.registry_dir}")
    
    def _load_index(self) -> None:
        """Load registry index from disk."""
        index_path = self.registry_dir / "index.json"
        
        if index_path.exists():
            with open(index_path) as f:
                data = json.load(f)
            self._next_id = data.get("next_id", 1)
            
            # Load all schemas into cache
            for schema_id_str, schema_path in data.get("schemas", {}).items():
                schema_id = int(schema_id_str)
                full_path = self.registry_dir / schema_path
                if full_path.exists():
                    with open(full_path) as f:
                        self._schema_cache[schema_id] = json.load(f)
            
            logger.debug(f"Loaded {len(self._schema_cache)} schemas from index")
    
    def _save_index(self) -> None:
        """Save registry index to disk."""
        index_path = self.registry_dir / "index.json"
        
        # Build schema path mapping
        schemas = {}
        for schema_id in self._schema_cache:
            schemas[str(schema_id)] = f"schemas/{schema_id}.json"
        
        data = {
            "next_id": self._next_id,
            "schemas": schemas,
        }
        
        with open(index_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def _get_subject_path(self, subject: str) -> Path:
        """Get path to subject metadata file."""
        # Sanitize subject name for filesystem
        safe_name = subject.replace("/", "_").replace("\\", "_")
        return self.subjects_dir / f"{safe_name}.json"
    
    def _load_subject(self, subject: str) -> Dict:
        """Load subject metadata."""
        if subject in self._subject_cache:
            return self._subject_cache[subject]
        
        subject_path = self._get_subject_path(subject)
        if subject_path.exists():
            with open(subject_path) as f:
                data = json.load(f)
            self._subject_cache[subject] = data
            return data
        
        return {
            "subject": subject,
            "compatibility": self.default_compatibility.value,
            "versions": [],
        }
    
    def _save_subject(self, subject: str, data: Dict) -> None:
        """Save subject metadata."""
        self._subject_cache[subject] = data
        subject_path = self._get_subject_path(subject)
        with open(subject_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def _compute_fingerprint(self, schema: Dict[str, Any]) -> str:
        """Compute schema fingerprint."""
        import hashlib
        canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]
    
    def register(
        self,
        subject: str,
        schema: Dict[str, Any],
    ) -> int:
        """
        Register a new schema version.
        
        Args:
            subject: Schema subject name
            schema: Avro schema dictionary
            
        Returns:
            Schema ID
            
        Raises:
            ValueError: If schema is not compatible
        """
        with self._lock:
            # Check compatibility
            result = self.check_compatibility(subject, schema)
            if not result.is_compatible:
                raise ValueError(
                    f"Schema not compatible with {subject}: {result.summary()}"
                )
            
            # Check for duplicate
            fingerprint = self._compute_fingerprint(schema)
            subject_data = self._load_subject(subject)
            
            for version_info in subject_data["versions"]:
                if version_info["fingerprint"] == fingerprint:
                    logger.debug(f"Schema already registered as ID {version_info['id']}")
                    return version_info["id"]
            
            # Assign new ID
            schema_id = self._next_id
            self._next_id += 1
            
            # Store schema
            self._schema_cache[schema_id] = schema
            schema_path = self.schemas_dir / f"{schema_id}.json"
            with open(schema_path, "w") as f:
                json.dump(schema, f, indent=2)
            
            # Update subject metadata
            version = len(subject_data["versions"]) + 1
            subject_data["versions"].append({
                "version": version,
                "id": schema_id,
                "fingerprint": fingerprint,
                "registered_at": datetime.now(timezone.utc).isoformat(),
            })
            self._save_subject(subject, subject_data)
            
            # Update index
            self._save_index()
            
            logger.info(f"Registered schema {subject} v{version} with ID {schema_id}")
            return schema_id
    
    def get_schema(self, schema_id: int) -> Optional[Dict[str, Any]]:
        """Get schema by ID."""
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        
        # Try loading from disk
        schema_path = self.schemas_dir / f"{schema_id}.json"
        if schema_path.exists():
            with open(schema_path) as f:
                schema = json.load(f)
            self._schema_cache[schema_id] = schema
            return schema
        
        return None
    
    def get_latest(self, subject: str) -> Optional[SchemaMetadata]:
        """Get latest schema version for subject."""
        subject_data = self._load_subject(subject)
        versions = subject_data.get("versions", [])
        
        if not versions:
            return None
        
        latest = versions[-1]
        schema = self.get_schema(latest["id"])
        
        if schema is None:
            return None
        
        return SchemaMetadata(
            schema_id=latest["id"],
            subject=subject,
            version=latest["version"],
            schema=schema,
            fingerprint=latest["fingerprint"],
            registered_at=datetime.fromisoformat(latest["registered_at"]),
        )
    
    def get_version(self, subject: str, version: int) -> Optional[SchemaMetadata]:
        """Get specific version of a schema."""
        subject_data = self._load_subject(subject)
        
        for v in subject_data.get("versions", []):
            if v["version"] == version:
                schema = self.get_schema(v["id"])
                if schema is None:
                    return None
                
                return SchemaMetadata(
                    schema_id=v["id"],
                    subject=subject,
                    version=v["version"],
                    schema=schema,
                    fingerprint=v["fingerprint"],
                    registered_at=datetime.fromisoformat(v["registered_at"]),
                )
        
        return None
    
    def get_versions(self, subject: str) -> List[int]:
        """Get all version numbers for a subject."""
        subject_data = self._load_subject(subject)
        return [v["version"] for v in subject_data.get("versions", [])]
    
    def get_subjects(self) -> List[str]:
        """Get all registered subjects."""
        subjects = []
        for path in self.subjects_dir.glob("*.json"):
            with open(path) as f:
                data = json.load(f)
            subjects.append(data.get("subject", path.stem))
        return subjects
    
    def check_compatibility(
        self,
        subject: str,
        schema: Dict[str, Any],
    ) -> CompatibilityResult:
        """Check if schema is compatible with latest version."""
        latest = self.get_latest(subject)
        
        if latest is None:
            # No existing schema = compatible
            return CompatibilityResult(
                is_compatible=True,
                level=self.get_compatibility(subject),
                issues=[],
            )
        
        level = self.get_compatibility(subject)
        checker = CompatibilityChecker(level)
        return checker.check(schema, latest.schema)
    
    def set_compatibility(
        self,
        subject: str,
        level: CompatibilityLevel,
    ) -> None:
        """Set compatibility level for a subject."""
        subject_data = self._load_subject(subject)
        subject_data["compatibility"] = level.value
        self._save_subject(subject, subject_data)
        logger.info(f"Set compatibility for {subject} to {level.value}")
    
    def get_compatibility(self, subject: str) -> CompatibilityLevel:
        """Get compatibility level for a subject."""
        subject_data = self._load_subject(subject)
        level_str = subject_data.get("compatibility", self.default_compatibility.value)
        return CompatibilityLevel(level_str)
    
    def delete_subject(self, subject: str) -> bool:
        """
        Delete a subject and all its versions.
        
        WARNING: This is destructive!
        """
        subject_path = self._get_subject_path(subject)
        
        if not subject_path.exists():
            return False
        
        with self._lock:
            # Remove from cache
            if subject in self._subject_cache:
                del self._subject_cache[subject]
            
            # Delete file
            subject_path.unlink()
            
            logger.warning(f"Deleted subject {subject}")
            return True
    
    def get_all_schemas(self) -> Dict[str, List[SchemaMetadata]]:
        """Get all schemas organized by subject."""
        result = {}
        for subject in self.get_subjects():
            result[subject] = []
            for version in self.get_versions(subject):
                metadata = self.get_version(subject, version)
                if metadata:
                    result[subject].append(metadata)
        return result
