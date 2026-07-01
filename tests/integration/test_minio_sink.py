"""
Integration Tests for MinIO Sink
=================================
Tests the MinIO sink with a real MinIO instance.

Requires Docker services to be running:
    cd docker && docker-compose up -d minio

Run with: pytest tests/integration/test_minio_sink.py -v
"""

import json
import time
import uuid
from io import BytesIO

import pytest

from consumer.minio_sink import MinIOSink
from schemas.cdc_event import CDCEvent, OperationType, SourceInfo


@pytest.fixture
def minio_sink(require_docker):
    """Create a real MinIO sink for testing."""
    import src.consumer.config as config
    
    original_endpoint = config.MINIO_ENDPOINT
    config.MINIO_ENDPOINT = "127.0.0.1:9000"
    
    sink = MinIOSink()
    
    yield sink
    
    # Cleanup and restore config
    sink.close()
    config.MINIO_ENDPOINT = original_endpoint


@pytest.fixture
def test_source_info():
    """SourceInfo for test table."""
    return SourceInfo(
        database="source_db",
        schema_name="public",
        table="test_table",
    )


@pytest.mark.integration
class TestMinIOSinkConnection:
    """Tests for MinIO connection handling."""

    def test_sink_initializes(self, minio_sink):
        """Test that sink initializes correctly."""
        assert minio_sink.client is not None

    def test_bucket_exists(self, minio_sink):
        """Test that Bronze bucket exists."""
        assert minio_sink.client.bucket_exists("cdc-bronze")


@pytest.mark.integration
class TestMinIOSinkWrite:
    """Tests for writing events to MinIO."""

    def test_write_single_event(self, minio_sink, test_source_info, minio_client):
        """Test writing a single event."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=test_source_info,
            ts_ms=1234567890000,
            before=None,
            after={"id": 1, "name": "Test"},
        )
        
        # Write event (goes to buffer)
        minio_sink.write(event)
        
        # Force flush
        minio_sink.flush_all()
        
        # Give a moment for write to complete
        time.sleep(0.5)
        
        # Check that file exists in MinIO
        objects = list(minio_client.list_objects("cdc-bronze", prefix="test_table/", recursive=True))
        
        assert len(objects) >= 1

    def test_write_batch_events(self, minio_sink, test_source_info, minio_client):
        """Test writing a batch of events."""
        events = []
        for i in range(5):
            events.append(CDCEvent(
                event_id=str(uuid.uuid4()),
                operation=OperationType.INSERT,
                source=test_source_info,
                ts_ms=1234567890000,
                before=None,
                after={"id": i, "name": f"Batch{i}"},
            ))
        
        # Write all events
        for event in events:
            minio_sink.write(event)
        
        # Force flush
        minio_sink.flush_all()
        time.sleep(0.5)
        
        # Check objects exist
        objects = list(minio_client.list_objects("cdc-bronze", prefix="test_table/", recursive=True))
        
        assert len(objects) >= 1

    def test_events_persisted_as_jsonl(self, minio_sink, test_source_info, minio_client):
        """Test that events are written as JSON Lines format."""
        unique_name = f"jsonl_test_{uuid.uuid4().hex[:8]}"
        
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=test_source_info,
            ts_ms=1234567890000,
            before=None,
            after={"id": 1, "name": unique_name},
        )
        
        minio_sink.write(event)
        minio_sink.flush_all()
        time.sleep(0.5)
        
        # Find and read the file
        objects = list(minio_client.list_objects("cdc-bronze", prefix="test_table/", recursive=True))
        
        # Read the most recent file
        if objects:
            obj = objects[-1]
            response = minio_client.get_object("cdc-bronze", obj.object_name)
            content = response.read().decode("utf-8")
            response.close()
            
            # Should be valid JSON Lines
            for line in content.strip().split("\n"):
                if line:
                    data = json.loads(line)
                    assert "operation" in data
                    assert "source" in data


@pytest.mark.integration
class TestMinIOSinkBuffer:
    """Tests for buffer management."""

    def test_buffer_size_tracking(self, minio_sink, test_source_info):
        """Test that buffer sizes are tracked."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=test_source_info,
            ts_ms=1234567890000,
            before=None,
            after={"id": 1, "name": "Buffer Test"},
        )
        
        # Write event
        minio_sink.write(event)
        
        # Check buffer size
        sizes = minio_sink.get_buffer_sizes()
        
        assert "test_table" in sizes
        assert sizes["test_table"] >= 1

    def test_flush_clears_buffer(self, minio_sink, test_source_info):
        """Test that flush clears the buffer."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=test_source_info,
            ts_ms=1234567890000,
            before=None,
            after={"id": 1, "name": "Flush Test"},
        )
        
        minio_sink.write(event)
        assert minio_sink.get_buffer_sizes().get("test_table", 0) >= 1
        
        minio_sink.flush_all()
        time.sleep(0.5)
        
        # Buffer should be empty after flush
        assert minio_sink.get_buffer_sizes().get("test_table", 0) == 0


@pytest.mark.integration
class TestMinIOSinkPartitioning:
    """Tests for path partitioning."""

    def test_path_contains_date_partitions(self, minio_sink, test_source_info, minio_client):
        """Test that paths contain date-based partitions."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=test_source_info,
            ts_ms=1234567890000,
            before=None,
            after={"id": 1, "name": "Partition Test"},
        )
        
        minio_sink.write(event)
        minio_sink.flush_all()
        time.sleep(0.5)
        
        # Check path structure
        objects = list(minio_client.list_objects("cdc-bronze", prefix="test_table/", recursive=True))
        
        if objects:
            path = objects[-1].object_name
            # Path should be: test_table/YYYY/MM/DD/HH/events_xxx.jsonl
            parts = path.split("/")
            assert len(parts) >= 5  # table/year/month/day/hour/file
            assert parts[0] == "test_table"
            assert parts[1].isdigit()  # year
            assert parts[2].isdigit()  # month


@pytest.mark.integration
class TestMinIOSinkMultiTable:
    """Tests for handling multiple tables."""

    def test_separate_buffers_per_table(self, minio_sink):
        """Test that each table has its own buffer."""
        # Create events for different tables
        for table in ["customers", "orders", "products"]:
            source = SourceInfo(
                database="source_db",
                schema_name="public",
                table=table,
            )
            
            event = CDCEvent(
                event_id=str(uuid.uuid4()),
                operation=OperationType.INSERT,
                source=source,
                ts_ms=1234567890000,
                before=None,
                after={"id": 1, "name": f"Test {table}"},
            )
            
            minio_sink.write(event)
        
        # Check buffer sizes
        sizes = minio_sink.get_buffer_sizes()
        
        assert "customers" in sizes
        assert "orders" in sizes
        assert "products" in sizes
