"""
CDC Event Schema - Pydantic Models
====================================
Defines the structure of CDC events that flow through our pipeline.
These models serve as:
  1. Data validation (reject malformed events)
  2. Documentation (schema is self-describing)
  3. Serialization/Deserialization (to/from JSON)
  4. Type safety (IDE autocomplete, mypy checks)

Every CDC event follows this structure:
  - Envelope (metadata about the event)
  - Source (where the change came from)
  - Payload (the actual data: before and after states)
"""

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ============================================================
# Enums
# ============================================================

class OperationType(str, Enum):
    """The type of database operation that produced this event.

    INSERT = New row created
    UPDATE = Existing row modified
    DELETE = Existing row removed
    """
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


# ============================================================
# Source Metadata
# ============================================================

class SourceInfo(BaseModel):
    """Information about WHERE the change came from.

    This tells the consumer exactly which database, schema, and table
    produced this event, plus the WAL position for deduplication.
    """
    # Which database instance (useful when you have multiple sources)
    database: str = Field(description="Source database name")

    # PostgreSQL schema (usually 'public')
    schema_name: str = Field(alias="schema", description="Database schema name")

    # Which table changed
    table: str = Field(description="Table name that was modified")

    # PostgreSQL transaction ID (groups related changes in same transaction)
    transaction_id: Optional[int] = Field(
        default=None,
        description="PostgreSQL transaction ID for grouping related changes",
    )

    # Log Sequence Number - unique position in WAL
    # Used for exactly-once processing: if we've seen this LSN, skip it
    lsn: Optional[str] = Field(
        default=None,
        description="WAL Log Sequence Number (unique position in the write-ahead log)",
    )

    class Config:
        populate_by_name = True


# ============================================================
# CDC Event (Main Model)
# ============================================================

class CDCEvent(BaseModel):
    """A single Change Data Capture event.

    This is the message that flows through Kafka, from producer to consumer.
    Every INSERT, UPDATE, or DELETE on a source table produces one CDCEvent.

    Examples:
        INSERT: before=None, after={full row data}
        UPDATE: before={old values}, after={new values}
        DELETE: before={full row data}, after=None
    """

    # Unique event identifier (UUID v4)
    # Used for deduplication: process each event_id exactly once
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique event ID for deduplication",
    )

    # When the change happened (UTC)
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the change occurred",
    )

    # Source metadata (database, schema, table, LSN)
    source: SourceInfo = Field(description="Source database metadata")

    # What type of change: INSERT, UPDATE, or DELETE
    operation: OperationType = Field(description="Type of database operation")

    # Row state BEFORE the change
    # - INSERT: None (row didn't exist before)
    # - UPDATE: Old column values
    # - DELETE: Full row data before deletion
    before: Optional[dict[str, Any]] = Field(
        default=None,
        description="Row data before the change (null for INSERT)",
    )

    # Row state AFTER the change
    # - INSERT: Full new row data
    # - UPDATE: New column values
    # - DELETE: None (row no longer exists)
    after: Optional[dict[str, Any]] = Field(
        default=None,
        description="Row data after the change (null for DELETE)",
    )

    def get_partition_key(self) -> str:
        """Extract the primary key value for Kafka partition routing.

        Uses the entity's 'id' field from either 'after' (INSERT/UPDATE)
        or 'before' (DELETE) to ensure all events for the same entity
        go to the same Kafka partition.
        """
        data = self.after or self.before
        if data and "id" in data:
            return str(data["id"])
        # Fallback: use event_id (random distribution)
        return self.event_id

    def to_json_bytes(self) -> bytes:
        """Serialize the event to JSON bytes for Kafka production."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> "CDCEvent":
        """Deserialize a CDC event from Kafka message bytes."""
        return cls.model_validate_json(data)

    def is_insert(self) -> bool:
        return self.operation == OperationType.INSERT

    def is_update(self) -> bool:
        return self.operation == OperationType.UPDATE

    def is_delete(self) -> bool:
        return self.operation == OperationType.DELETE


# ============================================================
# Dead Letter Queue Event (wraps a failed CDC event)
# ============================================================

class DLQEvent(BaseModel):
    """A CDC event that failed processing and was sent to the Dead Letter Queue.

    Contains the original event plus metadata about why it failed.
    Operations teams use this to investigate and replay failed events.
    """

    # The original event that failed
    original_event: CDCEvent = Field(description="The CDC event that failed processing")

    # Error information
    error_message: str = Field(description="Human-readable error description")
    error_type: str = Field(description="Exception class name")

    # Processing metadata
    retry_count: int = Field(default=0, description="Number of times processing was attempted")
    failed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the event was sent to DLQ",
    )
    failed_sink: Optional[str] = Field(
        default=None,
        description="Which sink failed (minio, postgres, or multiple)",
    )
    consumer_id: Optional[str] = Field(
        default=None,
        description="Which consumer instance failed to process this",
    )

    def to_json_bytes(self) -> bytes:
        """Serialize the DLQ event to JSON bytes."""
        return self.model_dump_json().encode("utf-8")


# ============================================================
# Factory Functions (helpers for creating events)
# ============================================================

def create_insert_event(
    database: str,
    schema: str,
    table: str,
    row_data: dict[str, Any],
    transaction_id: Optional[int] = None,
    lsn: Optional[str] = None,
) -> CDCEvent:
    """Create a CDC event for an INSERT operation."""
    return CDCEvent(
        source=SourceInfo(
            database=database,
            schema=schema,
            table=table,
            transaction_id=transaction_id,
            lsn=lsn,
        ),
        operation=OperationType.INSERT,
        before=None,
        after=row_data,
    )


def create_update_event(
    database: str,
    schema: str,
    table: str,
    old_data: dict[str, Any],
    new_data: dict[str, Any],
    transaction_id: Optional[int] = None,
    lsn: Optional[str] = None,
) -> CDCEvent:
    """Create a CDC event for an UPDATE operation."""
    return CDCEvent(
        source=SourceInfo(
            database=database,
            schema=schema,
            table=table,
            transaction_id=transaction_id,
            lsn=lsn,
        ),
        operation=OperationType.UPDATE,
        before=old_data,
        after=new_data,
    )


def create_delete_event(
    database: str,
    schema: str,
    table: str,
    row_data: dict[str, Any],
    transaction_id: Optional[int] = None,
    lsn: Optional[str] = None,
) -> CDCEvent:
    """Create a CDC event for a DELETE operation."""
    return CDCEvent(
        source=SourceInfo(
            database=database,
            schema=schema,
            table=table,
            transaction_id=transaction_id,
            lsn=lsn,
        ),
        operation=OperationType.DELETE,
        before=row_data,
        after=None,
    )
