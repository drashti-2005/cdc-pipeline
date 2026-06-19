"""
Unit Tests for CDC Event Schema
================================
Tests the Pydantic models for CDC events.

Run with: pytest tests/unit/test_cdc_event.py -v
"""

import json
import uuid
from datetime import datetime, timezone

import pytest

from src.schemas.cdc_event import (
    CDCEvent,
    DLQEvent,
    OperationType,
    SourceInfo,
)


class TestSourceInfo:
    """Tests for SourceInfo model."""

    def test_create_source_info(self):
        """Test creating a valid SourceInfo."""
        source = SourceInfo(
            database="source_db",
            schema_name="public",
            table="customers",
            transaction_id=12345,
            lsn="0/ABC123",
        )
        
        assert source.table == "customers"
        assert source.schema_name == "public"
        assert source.database == "source_db"

    def test_source_info_default_values(self):
        """Test SourceInfo with minimal required fields."""
        source = SourceInfo(
            database="testdb",
            schema_name="public",
            table="test_table",
        )
        
        assert source.transaction_id is None
        assert source.lsn is None


class TestOperationType:
    """Tests for OperationType enum."""

    def test_operation_types_exist(self):
        """Verify all expected operation types exist."""
        assert OperationType.INSERT.value == "INSERT"
        assert OperationType.UPDATE.value == "UPDATE"
        assert OperationType.DELETE.value == "DELETE"

    def test_operation_from_string(self):
        """Test creating OperationType from string."""
        assert OperationType("INSERT") == OperationType.INSERT
        assert OperationType("UPDATE") == OperationType.UPDATE
        assert OperationType("DELETE") == OperationType.DELETE

    def test_invalid_operation_raises(self):
        """Test that invalid operation raises ValueError."""
        with pytest.raises(ValueError):
            OperationType("INVALID")


class TestCDCEvent:
    """Tests for CDCEvent model."""

    def test_create_insert_event(self, sample_source_info, sample_customer_data):
        """Test creating an INSERT event."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=sample_source_info,
            before=None,
            after=sample_customer_data,
        )
        
        assert event.operation == OperationType.INSERT
        assert event.before is None
        assert event.after == sample_customer_data
        assert event.after["first_name"] == "John"

    def test_create_update_event(self, sample_source_info, sample_customer_data):
        """Test creating an UPDATE event."""
        before = sample_customer_data.copy()
        after = sample_customer_data.copy()
        after["email"] = "updated@example.com"
        
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.UPDATE,
            source=sample_source_info,
            before=before,
            after=after,
        )
        
        assert event.operation == OperationType.UPDATE
        assert event.before["email"] == "john.doe@example.com"
        assert event.after["email"] == "updated@example.com"

    def test_create_delete_event(self, sample_source_info, sample_customer_data):
        """Test creating a DELETE event."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.DELETE,
            source=sample_source_info,
            before=sample_customer_data,
            after=None,
        )
        
        assert event.operation == OperationType.DELETE
        assert event.before is not None
        assert event.after is None

    def test_event_id_is_unique(self, sample_insert_event):
        """Test that event_id is a valid UUID."""
        # Should not raise
        uuid.UUID(sample_insert_event.event_id)

    def test_get_partition_key(self, sample_insert_event):
        """Test partition key generation."""
        key = sample_insert_event.get_partition_key()
        
        # Key should be based on the 'id' field
        assert key == "1"

    def test_get_kafka_topic(self, sample_insert_event):
        """Test Kafka topic generation."""
        topic = sample_insert_event.get_kafka_topic()
        
        assert topic == "cdc.source_db.public.customers"

    def test_event_serialization(self, sample_insert_event):
        """Test JSON serialization."""
        json_str = sample_insert_event.model_dump_json()
        
        # Should be valid JSON
        data = json.loads(json_str)
        
        assert data["operation"] == "INSERT"
        assert data["source"]["table"] == "customers"
        assert data["after"]["first_name"] == "John"

    def test_event_deserialization(self, sample_insert_event):
        """Test JSON deserialization."""
        json_str = sample_insert_event.model_dump_json()
        
        # Recreate from JSON
        restored = CDCEvent.model_validate_json(json_str)
        
        assert restored.event_id == sample_insert_event.event_id
        assert restored.operation == sample_insert_event.operation
        assert restored.after == sample_insert_event.after


class TestDLQEvent:
    """Tests for Dead Letter Queue event model."""

    def test_create_dlq_event(self, sample_insert_event):
        """Test creating a DLQ event."""
        dlq_event = DLQEvent(
            original_event=sample_insert_event,
            error_message="Test error",
            error_type="TestError",
            failed_sink="postgres",
            retry_count=3,
        )
        
        assert dlq_event.original_event == sample_insert_event
        assert dlq_event.error_message == "Test error"
        assert dlq_event.retry_count == 3

    def test_dlq_event_serialization(self, sample_insert_event):
        """Test DLQ event JSON serialization."""
        dlq_event = DLQEvent(
            original_event=sample_insert_event,
            error_message="Connection timeout",
            error_type="ConnectionError",
            failed_sink="minio",
            retry_count=5,
        )
        
        json_str = dlq_event.model_dump_json()
        data = json.loads(json_str)
        
        assert data["error_type"] == "ConnectionError"
        assert data["original_event"]["operation"] == "INSERT"


class TestEventBatch:
    """Tests for batch operations on events."""

    def test_batch_has_mixed_operations(self, sample_events_batch):
        """Test that batch contains different operation types."""
        operations = [e.operation for e in sample_events_batch]
        
        assert OperationType.INSERT in operations
        assert OperationType.UPDATE in operations
        assert OperationType.DELETE in operations

    def test_batch_size(self, sample_events_batch):
        """Test batch size is as expected."""
        assert len(sample_events_batch) == 6  # 3 INSERT + 2 UPDATE + 1 DELETE

    def test_batch_serialization(self, sample_events_batch):
        """Test serializing entire batch to JSON Lines."""
        lines = [e.model_dump_json() for e in sample_events_batch]
        content = "\n".join(lines)
        
        # Should have 6 lines
        assert len(content.split("\n")) == 6
        
        # Each line should be valid JSON
        for line in content.split("\n"):
            json.loads(line)  # Should not raise
