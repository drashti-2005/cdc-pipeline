"""
Avro Serializer - Local Mode (No Schema Registry Required)
==========================================================
Serializes CDC events to Avro binary format using local schema files.

WHY LOCAL MODE?
---------------
Schema Registry requires a running service (Confluent/Karapace).
This module provides the SAME serialization without external dependencies.

WHEN TO USE:
- Development on Windows/Mac where Docker has issues
- Unit tests that don't need a registry
- Learning Avro concepts
- CI/CD pipelines without infrastructure

PRODUCTION UPGRADE:
When you have Schema Registry available, switch to schema_registry.py
which adds:
- Schema versioning and evolution
- Compatibility checking
- Central schema catalog

AVRO FORMAT EXPLAINED:
----------------------
JSON:  {"name": "John", "age": 30}     → 27 bytes (human readable)
Avro:  [binary data]                    → 8 bytes (compact, typed)

Avro encodes data WITHOUT field names (schema defines structure).
This is why it's 50-70% smaller than JSON.

FOR INTERVIEWS:
---------------
Q: Why use Avro over JSON in Kafka?
A: 1. Smaller messages (no field names repeated)
   2. Faster parsing (binary vs text)
   3. Schema enforcement (catch errors at serialization)
   4. Schema evolution support (backward/forward compatibility)
"""

import io
import json
import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import fastavro

logger = logging.getLogger(__name__)

# Path to Avro schema files
AVRO_SCHEMAS_DIR = Path(__file__).parent / "avro"


class LocalAvroSerializer:
    """
    Serialize/deserialize data using local Avro schemas.
    
    No Schema Registry needed - uses .avsc files directly.
    
    SIMPLE EXPLANATION:
    Think of this like a stamp maker:
    - The schema is the stamp design
    - Data is the ink
    - Output is a perfectly formatted impression
    
    Without the schema (design), you can't read the impression.
    That's why both sides need the same schema.
    """
    
    def __init__(self, schemas_dir: Optional[Path] = None):
        """
        Initialize with path to schema files.
        
        Args:
            schemas_dir: Directory containing .avsc files
        """
        self.schemas_dir = schemas_dir or AVRO_SCHEMAS_DIR
        self._parsed_schemas: dict[str, dict] = {}
        
        logger.info(f"LocalAvroSerializer initialized with schemas from {self.schemas_dir}")
    
    @lru_cache(maxsize=20)
    def load_schema(self, schema_name: str) -> dict:
        """
        Load and parse an Avro schema from a .avsc file.
        
        Args:
            schema_name: Name of schema (without .avsc extension)
            
        Returns:
            Parsed Avro schema dictionary
        """
        schema_path = self.schemas_dir / f"{schema_name}.avsc"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
        
        with open(schema_path, "r") as f:
            schema = json.load(f)
        
        # Parse schema for fastavro (validates and normalizes)
        parsed = fastavro.parse_schema(schema)
        
        logger.debug(f"Loaded schema: {schema_name}")
        return parsed
    
    def serialize(self, data: dict, schema_name: str) -> bytes:
        """
        Serialize a Python dict to Avro binary.
        
        Args:
            data: Dictionary to serialize
            schema_name: Name of the Avro schema to use
            
        Returns:
            Avro binary bytes
            
        Example:
            >>> serializer = LocalAvroSerializer()
            >>> data = {"event_id": "123", "operation": "INSERT", ...}
            >>> binary = serializer.serialize(data, "cdc_event")
            >>> len(binary)  # Much smaller than JSON
            45
        """
        schema = self.load_schema(schema_name)
        
        # Write to binary buffer
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema, data)
        
        return buffer.getvalue()
    
    def deserialize(self, data: bytes, schema_name: str) -> dict:
        """
        Deserialize Avro binary to Python dict.
        
        Args:
            data: Avro binary bytes
            schema_name: Name of the schema used for serialization
            
        Returns:
            Deserialized Python dictionary
        """
        schema = self.load_schema(schema_name)
        
        buffer = io.BytesIO(data)
        return fastavro.schemaless_reader(buffer, schema)
    
    def serialize_with_header(
        self,
        data: dict,
        schema_name: str,
        schema_id: int = 1,
    ) -> bytes:
        """
        Serialize with Schema Registry-compatible header.
        
        Adds a 5-byte header:
        - Byte 0: Magic byte (0x00)
        - Bytes 1-4: Schema ID (big-endian)
        
        This format is compatible with Confluent Schema Registry.
        When you switch to a real registry, consumers can read both.
        
        Args:
            data: Dictionary to serialize
            schema_name: Name of the schema
            schema_id: Fake schema ID (use real ID with registry)
            
        Returns:
            Header + Avro binary
        """
        # Serialize data
        avro_bytes = self.serialize(data, schema_name)
        
        # Create header: magic byte + schema ID
        header = bytes([0x00]) + schema_id.to_bytes(4, "big")
        
        return header + avro_bytes
    
    def deserialize_with_header(
        self,
        data: bytes,
        schema_name: str,
    ) -> tuple[int, dict]:
        """
        Deserialize data with Schema Registry header.
        
        Args:
            data: Header + Avro binary
            schema_name: Schema to use for deserialization
            
        Returns:
            Tuple of (schema_id, deserialized_data)
        """
        # Parse header
        if len(data) < 5 or data[0] != 0x00:
            raise ValueError("Invalid Avro message: missing magic byte")
        
        schema_id = int.from_bytes(data[1:5], "big")
        avro_bytes = data[5:]
        
        # Deserialize
        result = self.deserialize(avro_bytes, schema_name)
        
        return schema_id, result
    
    def validate(self, data: dict, schema_name: str) -> bool:
        """
        Validate data against a schema without serializing.
        
        Args:
            data: Dictionary to validate
            schema_name: Schema to validate against
            
        Returns:
            True if valid
            
        Raises:
            ValueError if invalid
        """
        schema = self.load_schema(schema_name)
        
        try:
            # Try to serialize - if it works, data is valid
            buffer = io.BytesIO()
            fastavro.schemaless_writer(buffer, schema, data)
            return True
        except Exception as e:
            raise ValueError(f"Validation failed: {e}")


# ============================================================
# Convenience Functions
# ============================================================

_serializer: Optional[LocalAvroSerializer] = None


def get_serializer() -> LocalAvroSerializer:
    """Get or create the global serializer instance."""
    global _serializer
    if _serializer is None:
        _serializer = LocalAvroSerializer()
    return _serializer


def cdc_event_to_avro(event: dict) -> bytes:
    """
    Convert a CDC event dict to Avro bytes.
    
    Args:
        event: CDC event dictionary with fields matching cdc_event.avsc
        
    Returns:
        Avro binary bytes
    """
    return get_serializer().serialize(event, "cdc_event")


def avro_to_cdc_event(data: bytes) -> dict:
    """
    Convert Avro bytes to CDC event dict.
    
    Args:
        data: Avro binary bytes
        
    Returns:
        CDC event dictionary
    """
    return get_serializer().deserialize(data, "cdc_event")


def create_avro_cdc_event(
    event_id: str,
    operation: str,
    database: str,
    schema: str,
    table: str,
    before: Optional[dict] = None,
    after: Optional[dict] = None,
    transaction_id: Optional[int] = None,
    lsn: Optional[str] = None,
) -> dict:
    """
    Create a CDC event dict ready for Avro serialization.
    
    This helper ensures the dict matches the Avro schema structure.
    
    Args:
        event_id: Unique event identifier
        operation: INSERT, UPDATE, or DELETE
        database: Source database name
        schema: Database schema (e.g., 'public')
        table: Table name
        before: Row data before change (JSON string for Avro)
        after: Row data after change (JSON string for Avro)
        transaction_id: PostgreSQL transaction ID
        lsn: WAL Log Sequence Number
        
    Returns:
        Dict matching cdc_event.avsc schema
    """
    return {
        "event_id": event_id,
        "event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source": {
            "database": database,
            "schema": schema,
            "table": table,
            "transaction_id": transaction_id,
            "lsn": lsn,
        },
        "operation": operation,
        "before": json.dumps(before) if before else None,
        "after": json.dumps(after) if after else None,
        "schema_version": 1,
    }


# ============================================================
# Size Comparison Demo
# ============================================================

def compare_sizes():
    """
    Demo function showing JSON vs Avro size difference.
    
    Run with: python -c "from schemas.avro_serializer import compare_sizes; compare_sizes()"
    """
    import uuid
    
    # Create sample event
    event = create_avro_cdc_event(
        event_id=str(uuid.uuid4()),
        operation="INSERT",
        database="source_db",
        schema="public",
        table="customers",
        after={"id": 1, "name": "John Doe", "email": "john@example.com"},
    )
    
    # JSON size
    json_bytes = json.dumps(event).encode("utf-8")
    
    # Avro size
    avro_bytes = cdc_event_to_avro(event)
    
    savings = (1 - len(avro_bytes) / len(json_bytes)) * 100
    
    print("=" * 50)
    print("JSON vs Avro Size Comparison")
    print("=" * 50)
    print(f"JSON size:  {len(json_bytes):>5} bytes")
    print(f"Avro size:  {len(avro_bytes):>5} bytes")
    print(f"Savings:    {savings:>5.1f}%")
    print("=" * 50)
    
    # Verify round-trip
    restored = avro_to_cdc_event(avro_bytes)
    assert restored["event_id"] == event["event_id"]
    print("✓ Round-trip serialization successful!")


if __name__ == "__main__":
    compare_sizes()
