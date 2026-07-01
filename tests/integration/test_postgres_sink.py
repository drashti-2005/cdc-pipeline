"""
Integration Tests for PostgreSQL Sink
======================================
Tests the PostgreSQL sink with a real database.

Requires Docker services to be running:
    cd docker && docker-compose up -d postgres-target

Run with: pytest tests/integration/test_postgres_sink.py -v
"""

import uuid

import pytest

from consumer.postgres_sink import PostgresSink
from schemas.cdc_event import CDCEvent, OperationType, SourceInfo


@pytest.fixture
def postgres_sink(require_docker):
    """Create a real PostgreSQL sink for testing."""
    # Temporarily patch config to use test connection
    import src.consumer.config as config
    
    original_host = config.TARGET_PG_HOST
    original_port = config.TARGET_PG_PORT
    
    config.TARGET_PG_HOST = "127.0.0.1"
    config.TARGET_PG_PORT = 5435
    
    sink = PostgresSink()
    
    yield sink
    
    # Restore config
    config.TARGET_PG_HOST = original_host
    config.TARGET_PG_PORT = original_port


@pytest.fixture
def customer_source_info():
    """SourceInfo for customers table."""
    return SourceInfo(
        database="source_db",
        schema_name="public",
        table="customers",
    )


@pytest.fixture
def unique_customer_id():
    """Generate a unique customer ID to avoid conflicts."""
    # Use high IDs to avoid conflicts with seed data
    return 90000 + uuid.uuid4().int % 10000


@pytest.mark.integration
class TestPostgresSinkConnection:
    """Tests for PostgreSQL connection handling."""

    def test_sink_connects_successfully(self, postgres_sink):
        """Test that sink establishes connection."""
        assert postgres_sink._conn is not None
        assert not postgres_sink._conn.closed

    def test_reconnect_on_failure(self, postgres_sink):
        """Test reconnection after connection failure."""
        # Force close the connection
        postgres_sink._conn.close()
        
        # Create an event to trigger reconnection
        # The _get_cursor context manager should reconnect
        with postgres_sink._get_cursor() as cursor:
            cursor.execute("SELECT 1 as value")
            result = cursor.fetchone()
            # RealDictCursor returns dict, not tuple
            assert result["value"] == 1


@pytest.mark.integration
class TestPostgresSinkInsert:
    """Tests for INSERT operations."""

    def test_insert_customer(
        self,
        postgres_sink,
        customer_source_info,
        unique_customer_id,
        target_db_connection,
    ):
        """Test inserting a new customer."""
        customer_data = {
            "id": unique_customer_id,
            "first_name": "Test",
            "last_name": "User",
            "email": f"test{unique_customer_id}@example.com",
            "created_at": "2024-01-15T10:30:00Z",
        }
        
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=customer_source_info,
            ts_ms=1234567890000,
            before=None,
            after=customer_data,
        )
        
        # Write the event
        postgres_sink.write(event)
        
        # Verify it was inserted
        cursor = target_db_connection.cursor()
        cursor.execute(
            "SELECT first_name, last_name, email FROM public.customers WHERE id = %s",
            (unique_customer_id,)
        )
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "Test"
        assert result[1] == "User"
        
        # Cleanup
        cursor.execute("DELETE FROM public.customers WHERE id = %s", (unique_customer_id,))
        target_db_connection.commit()

    def test_insert_is_idempotent(
        self,
        postgres_sink,
        customer_source_info,
        unique_customer_id,
        target_db_connection,
    ):
        """Test that inserting same event twice doesn't fail (upsert)."""
        customer_data = {
            "id": unique_customer_id,
            "first_name": "Idempotent",
            "last_name": "Test",
            "email": f"idempotent{unique_customer_id}@example.com",
            "created_at": "2024-01-15T10:30:00Z",
        }
        
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=customer_source_info,
            ts_ms=1234567890000,
            before=None,
            after=customer_data,
        )
        
        # Write twice - should not raise
        postgres_sink.write(event)
        postgres_sink.write(event)
        
        # Should still have only one row
        cursor = target_db_connection.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM public.customers WHERE id = %s",
            (unique_customer_id,)
        )
        count = cursor.fetchone()[0]
        
        assert count == 1
        
        # Cleanup
        cursor.execute("DELETE FROM public.customers WHERE id = %s", (unique_customer_id,))
        target_db_connection.commit()


@pytest.mark.integration
class TestPostgresSinkUpdate:
    """Tests for UPDATE operations."""

    def test_update_customer(
        self,
        postgres_sink,
        customer_source_info,
        unique_customer_id,
        target_db_connection,
    ):
        """Test updating an existing customer."""
        # First, insert a customer
        cursor = target_db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO public.customers (id, first_name, last_name, email, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (unique_customer_id, "Before", "Update", f"before{unique_customer_id}@example.com")
        )
        target_db_connection.commit()
        
        # Create update event
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.UPDATE,
            source=customer_source_info,
            ts_ms=1234567890000,
            before={
                "id": unique_customer_id,
                "first_name": "Before",
                "last_name": "Update",
                "email": f"before{unique_customer_id}@example.com",
            },
            after={
                "id": unique_customer_id,
                "first_name": "After",
                "last_name": "Update",
                "email": f"after{unique_customer_id}@example.com",
            },
        )
        
        # Apply update
        postgres_sink.write(event)
        
        # Verify update
        cursor.execute(
            "SELECT first_name, email FROM public.customers WHERE id = %s",
            (unique_customer_id,)
        )
        result = cursor.fetchone()
        
        assert result[0] == "After"
        assert "after" in result[1]
        
        # Cleanup
        cursor.execute("DELETE FROM public.customers WHERE id = %s", (unique_customer_id,))
        target_db_connection.commit()


@pytest.mark.integration
class TestPostgresSinkDelete:
    """Tests for DELETE operations."""

    def test_delete_customer(
        self,
        postgres_sink,
        customer_source_info,
        unique_customer_id,
        target_db_connection,
    ):
        """Test deleting an existing customer."""
        # First, insert a customer
        cursor = target_db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO public.customers (id, first_name, last_name, email, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (unique_customer_id, "ToDelete", "User", f"delete{unique_customer_id}@example.com")
        )
        target_db_connection.commit()
        
        # Create delete event
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.DELETE,
            source=customer_source_info,
            ts_ms=1234567890000,
            before={
                "id": unique_customer_id,
                "first_name": "ToDelete",
                "last_name": "User",
            },
            after=None,
        )
        
        # Apply delete
        postgres_sink.write(event)
        
        # Verify deletion
        cursor.execute(
            "SELECT COUNT(*) FROM public.customers WHERE id = %s",
            (unique_customer_id,)
        )
        count = cursor.fetchone()[0]
        
        assert count == 0

    def test_delete_nonexistent_doesnt_fail(
        self,
        postgres_sink,
        customer_source_info,
    ):
        """Test that deleting a non-existent row doesn't fail."""
        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.DELETE,
            source=customer_source_info,
            ts_ms=1234567890000,
            before={"id": 999999, "first_name": "Ghost"},
            after=None,
        )
        
        # Should not raise
        postgres_sink.write(event)
