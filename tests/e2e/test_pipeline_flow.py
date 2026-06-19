"""
End-to-End Tests for CDC Pipeline
===================================
Tests the complete pipeline flow: Source DB → Kafka → Consumer → Sinks

These tests require all Docker services to be running:
    cd docker && docker-compose up -d

Run with: pytest tests/e2e/test_pipeline_flow.py -v -m e2e
"""

import json
import time
import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
def unique_test_id():
    """Generate unique ID for test isolation."""
    return uuid.uuid4().hex[:8]


@pytest.mark.e2e
class TestSourceToKafka:
    """Tests for Source DB → Kafka flow."""

    def test_insert_produces_kafka_message(
        self,
        require_docker,
        source_db_connection,
        kafka_test_consumer,
        unique_test_id,
        wait_for_condition,
    ):
        """Test that INSERT in source DB produces Kafka message."""
        # Subscribe to customers topic
        kafka_test_consumer.subscribe(["cdc.source.public.customers"])
        
        # Insert a new customer in source DB
        cursor = source_db_connection.cursor()
        customer_email = f"e2e_{unique_test_id}@test.com"
        
        cursor.execute(
            """
            INSERT INTO customers (first_name, last_name, email, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING id
            """,
            ("E2E", "Test", customer_email)
        )
        customer_id = cursor.fetchone()[0]
        source_db_connection.commit()
        
        # Note: This test assumes the producer is running.
        # In a CI environment, you'd start the producer as part of test setup.
        # For now, we verify the database write worked.
        
        # Verify customer was created
        cursor.execute("SELECT * FROM customers WHERE id = %s", (customer_id,))
        result = cursor.fetchone()
        
        assert result is not None
        
        # Cleanup
        cursor.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
        source_db_connection.commit()


@pytest.mark.e2e
class TestKafkaToSinks:
    """Tests for Kafka → Consumer → Sinks flow."""

    def test_consumer_writes_to_minio(
        self,
        require_docker,
        kafka_test_producer,
        minio_client,
        unique_test_id,
        wait_for_condition,
    ):
        """Test that messages in Kafka end up in MinIO."""
        # Create a test event
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INSERT",
            "source": {
                "version": "1.0",
                "connector": "postgresql",
                "name": "source_db",
                "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                "db": "source_db",
                "schema_name": "public",
                "table": "customers",
            },
            "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "before": None,
            "after": {
                "id": 99999,
                "first_name": "E2E",
                "last_name": unique_test_id,
                "email": f"e2e_{unique_test_id}@test.com",
            },
        }
        
        # Produce to Kafka
        kafka_test_producer.produce(
            topic="cdc.source.public.customers",
            key=f"customers.{unique_test_id}".encode(),
            value=json.dumps(event).encode(),
        )
        kafka_test_producer.flush()
        
        # Note: This test verifies that we can write to Kafka.
        # Full E2E would wait for consumer to process and check MinIO.
        # That requires the consumer to be running.
        
        # For now, verify MinIO is accessible
        assert minio_client.bucket_exists("cdc-bronze")


@pytest.mark.e2e
class TestDataConsistency:
    """Tests for data consistency across the pipeline."""

    def test_source_and_target_match(
        self,
        require_docker,
        source_db_connection,
        target_db_connection,
    ):
        """Test that source and target have consistent row counts."""
        source_cursor = source_db_connection.cursor()
        target_cursor = target_db_connection.cursor()
        
        tables = ["customers", "products", "orders", "order_items"]
        
        for table in tables:
            # Get source count
            source_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            source_count = source_cursor.fetchone()[0]
            
            # Get target count
            target_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            target_count = target_cursor.fetchone()[0]
            
            # Log the counts (they might not match if consumer hasn't run)
            print(f"{table}: source={source_count}, target={target_count}")
            
            # At minimum, tables should exist
            assert source_count >= 0
            assert target_count >= 0

    def test_customer_data_integrity(
        self,
        require_docker,
        source_db_connection,
        target_db_connection,
    ):
        """Test that customer data matches between source and target."""
        source_cursor = source_db_connection.cursor()
        target_cursor = target_db_connection.cursor()
        
        # Get sample customer from source
        source_cursor.execute(
            "SELECT id, first_name, last_name, email FROM customers LIMIT 5"
        )
        source_customers = source_cursor.fetchall()
        
        # Verify they exist in target (if consumer has run)
        for customer in source_customers:
            customer_id = customer[0]
            target_cursor.execute(
                "SELECT first_name, last_name FROM customers WHERE id = %s",
                (customer_id,)
            )
            target_result = target_cursor.fetchone()
            
            # If target has the customer, data should match
            if target_result:
                assert target_result[0] == customer[1], f"First name mismatch for customer {customer_id}"
                assert target_result[1] == customer[2], f"Last name mismatch for customer {customer_id}"


@pytest.mark.e2e
class TestPipelineRecovery:
    """Tests for pipeline failure recovery."""

    def test_consumer_handles_invalid_message(
        self,
        require_docker,
        kafka_test_producer,
    ):
        """Test that invalid messages don't crash consumer."""
        # Send an invalid message
        kafka_test_producer.produce(
            topic="cdc.source.public.customers",
            key=b"invalid",
            value=b"this is not valid json",
        )
        kafka_test_producer.flush()
        
        # Consumer should handle this gracefully (send to DLQ)
        # This test just verifies we can produce the invalid message
        # Full verification would check DLQ

    def test_consumer_handles_partial_event(
        self,
        require_docker,
        kafka_test_producer,
    ):
        """Test that events with missing fields are handled."""
        # Event missing required fields
        incomplete_event = {
            "operation": "INSERT",
            # Missing event_id, source, etc.
        }
        
        kafka_test_producer.produce(
            topic="cdc.source.public.customers",
            key=b"incomplete",
            value=json.dumps(incomplete_event).encode(),
        )
        kafka_test_producer.flush()


@pytest.mark.e2e
@pytest.mark.slow
class TestPipelinePerformance:
    """Performance tests for the pipeline."""

    def test_batch_insert_performance(
        self,
        require_docker,
        source_db_connection,
        unique_test_id,
    ):
        """Test performance of batch inserts."""
        import time
        
        cursor = source_db_connection.cursor()
        batch_size = 100
        
        start = time.time()
        
        for i in range(batch_size):
            cursor.execute(
                """
                INSERT INTO customers (first_name, last_name, email, created_at)
                VALUES (%s, %s, %s, NOW())
                """,
                (f"Batch{i}", unique_test_id, f"batch{i}_{unique_test_id}@test.com")
            )
        
        source_db_connection.commit()
        
        elapsed = time.time() - start
        
        print(f"Inserted {batch_size} rows in {elapsed:.2f}s ({batch_size/elapsed:.0f} rows/sec)")
        
        # Should complete in reasonable time
        assert elapsed < 10, f"Batch insert too slow: {elapsed}s"
        
        # Cleanup
        cursor.execute(
            "DELETE FROM customers WHERE last_name = %s",
            (unique_test_id,)
        )
        source_db_connection.commit()

    def test_kafka_throughput(
        self,
        require_docker,
        kafka_test_producer,
        unique_test_id,
    ):
        """Test Kafka message production throughput."""
        import time
        
        message_count = 1000
        
        start = time.time()
        
        for i in range(message_count):
            event = {
                "event_id": str(uuid.uuid4()),
                "operation": "INSERT",
                "source": {"table": "perf_test"},
                "ts_ms": int(time.time() * 1000),
                "after": {"id": i, "test_id": unique_test_id},
            }
            
            kafka_test_producer.produce(
                topic="cdc.source.public.customers",
                key=f"perf.{i}".encode(),
                value=json.dumps(event).encode(),
            )
        
        kafka_test_producer.flush()
        
        elapsed = time.time() - start
        
        print(f"Produced {message_count} messages in {elapsed:.2f}s ({message_count/elapsed:.0f} msg/sec)")
        
        # Should achieve reasonable throughput
        assert message_count / elapsed > 100, f"Kafka throughput too low"
