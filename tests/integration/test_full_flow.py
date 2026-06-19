"""
Integration Tests for Full CDC Flow
=====================================
Tests the complete pipeline with real services.

Test Categories:
1. Kafka message flow
2. Quality-aware processing
3. DLQ handling
4. End-to-end scenarios

Requires Docker services:
    cd docker && docker-compose up -d

Run with: pytest tests/integration/test_full_flow.py -v -m integration
"""

import json
import time
import uuid
from datetime import datetime, timezone

import pytest

from tests.integration.test_utils import (
    create_test_cdc_event,
    create_invalid_cdc_event,
    wait_for_condition,
)


# ============================================================
# Kafka Flow Tests
# ============================================================

@pytest.mark.integration
class TestKafkaMessageFlow:
    """Tests for Kafka message production and consumption."""
    
    def test_produce_and_consume_cdc_event(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test basic produce/consume flow."""
        # Create test topic
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic)
        
        # Produce a CDC event
        event_id = kafka_helper.produce_cdc_event(
            topic=topic,
            operation="INSERT",
            table="customers",
            data={"id": 1, "name": "Test Customer"},
        )
        
        # Consume and verify
        messages = kafka_helper.consume_messages(topic, count=1, timeout=10)
        
        assert len(messages) == 1
        assert messages[0]["event_id"] == event_id
        assert messages[0]["operation"] == "INSERT"
    
    def test_produce_batch_events(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test producing multiple events."""
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic)
        
        # Produce 5 events
        event_ids = []
        for i in range(5):
            eid = kafka_helper.produce_cdc_event(
                topic=topic,
                operation="INSERT",
                table="customers",
                data={"id": i + 1, "name": f"Customer {i + 1}"},
            )
            event_ids.append(eid)
        
        # Consume all
        messages = kafka_helper.consume_messages(topic, count=5, timeout=15)
        
        assert len(messages) == 5
        received_ids = [m["event_id"] for m in messages]
        assert set(event_ids) == set(received_ids)
    
    def test_message_ordering_within_partition(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that messages maintain order within a partition."""
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic, num_partitions=1)
        
        # Produce events with sequence numbers
        for i in range(10):
            kafka_helper.produce(
                topic=topic,
                value={"sequence": i, "event_id": str(uuid.uuid4())},
                key="same-key",  # Same key = same partition
            )
        
        # Consume and verify order
        messages = kafka_helper.consume_messages(topic, count=10, timeout=15)
        
        sequences = [m["sequence"] for m in messages]
        assert sequences == list(range(10)), "Messages should be in order"


# ============================================================
# Quality-Aware Processing Tests
# ============================================================

@pytest.mark.integration
class TestQualityAwareProcessing:
    """Tests for quality-aware event processing."""
    
    def test_valid_event_passes_quality_check(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that valid events pass quality checks."""
        from src.consumer.event_processor import QualityAwareProcessor
        from unittest.mock import MagicMock
        
        # Create processor with mocked router and DLQ
        mock_router = MagicMock()
        mock_router.route.return_value = True
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
            enable_quality_checks=True,
        )
        
        # Create valid event
        event = create_test_cdc_event(
            operation="INSERT",
            table="customers",
            data={"id": 1, "name": "Valid Customer"},
        )
        
        # Process
        result = processor.process_message(
            raw_value=json.dumps(event).encode(),
            topic="test-topic",
        )
        
        # Verify
        assert result.success is True
        assert result.stage == "complete"
        mock_router.route.assert_called_once()
        mock_dlq.send_quality_failure.assert_not_called()
    
    def test_invalid_operation_fails_quality_check(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that invalid operations fail quality checks."""
        from src.consumer.event_processor import QualityAwareProcessor
        from unittest.mock import MagicMock
        
        mock_router = MagicMock()
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
            enable_quality_checks=True,
        )
        
        # Create invalid event
        event = create_test_cdc_event()
        event["operation"] = "INVALID_OP"
        
        # Process
        result = processor.process_message(
            raw_value=json.dumps(event).encode(),
        )
        
        # Verify
        assert result.success is False
        assert result.stage == "quality"
        mock_dlq.send_quality_failure.assert_called_once()
        mock_router.route.assert_not_called()
    
    def test_malformed_json_fails_deserialization(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that malformed JSON triggers deserialization error."""
        from src.consumer.event_processor import QualityAwareProcessor
        from unittest.mock import MagicMock
        
        mock_router = MagicMock()
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Process malformed JSON
        result = processor.process_message(
            raw_value=b"not valid json {{{",
        )
        
        # Verify
        assert result.success is False
        assert result.stage == "deserialization"
        mock_dlq.send_deserialization_error.assert_called_once()


# ============================================================
# DLQ Handling Tests
# ============================================================

@pytest.mark.integration
class TestDLQHandling:
    """Tests for Dead Letter Queue handling."""
    
    def test_quality_failure_produces_dlq_message(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that quality failures are sent to DLQ."""
        from src.consumer.dlq_handler import DLQHandler
        
        # Create DLQ topic
        dlq_topic = f"test.dlq.{unique_test_id}"
        kafka_helper.create_topic(dlq_topic)
        
        # Create DLQ handler
        dlq = DLQHandler(
            dlq_topic=dlq_topic,
            kafka_servers="127.0.0.1:9092",
            consumer_id="test-consumer",
        )
        
        # Send a quality failure
        event = {"event_id": "test-123", "bad_field": "data"}
        quality_report = {
            "passed": False,
            "failure_details": [
                {"message": "Missing required field: operation"},
            ],
        }
        
        dlq.send_quality_failure(
            event=event,
            quality_report=quality_report,
            topic="source-topic",
            partition=0,
            offset=100,
        )
        dlq.flush()
        
        # Consume from DLQ and verify
        messages = kafka_helper.consume_messages(dlq_topic, count=1, timeout=10)
        
        assert len(messages) == 1
        assert messages[0]["failure_reason"] == "quality_failure"
        assert messages[0]["original_event"] == event
        assert messages[0]["quality_report"] == quality_report
    
    def test_sink_failure_produces_dlq_message(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that sink failures are sent to DLQ."""
        from src.consumer.dlq_handler import DLQHandler
        
        dlq_topic = f"test.dlq.{unique_test_id}"
        kafka_helper.create_topic(dlq_topic)
        
        dlq = DLQHandler(
            dlq_topic=dlq_topic,
            kafka_servers="127.0.0.1:9092",
        )
        
        # Send a sink failure
        event = {"event_id": "test-456"}
        
        dlq.send_sink_failure(
            event=event,
            sink_name="postgres",
            error=ConnectionError("Database unavailable"),
            retry_count=3,
            topic="source-topic",
            partition=1,
            offset=500,
        )
        dlq.flush()
        
        # Consume and verify
        messages = kafka_helper.consume_messages(dlq_topic, count=1, timeout=10)
        
        assert len(messages) == 1
        assert messages[0]["failure_reason"] == "sink_failure"
        assert messages[0]["error_details"]["sink"] == "postgres"
        assert messages[0]["retry_count"] == 3
    
    def test_dlq_preserves_original_offset(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test that DLQ entries preserve original message metadata."""
        from src.consumer.dlq_handler import DLQHandler
        
        dlq_topic = f"test.dlq.{unique_test_id}"
        kafka_helper.create_topic(dlq_topic)
        
        dlq = DLQHandler(dlq_topic=dlq_topic, kafka_servers="127.0.0.1:9092")
        
        # Send with specific offset
        dlq.send_quality_failure(
            event={"id": 1},
            quality_report={},
            topic="original.topic",
            partition=5,
            offset=12345,
        )
        dlq.flush()
        
        # Verify metadata preserved
        messages = kafka_helper.consume_messages(dlq_topic, count=1, timeout=10)
        
        assert messages[0]["original_topic"] == "original.topic"
        assert messages[0]["original_partition"] == 5
        assert messages[0]["original_offset"] == 12345


# ============================================================
# End-to-End Scenario Tests
# ============================================================

@pytest.mark.integration
class TestEndToEndScenarios:
    """End-to-end tests for complete pipeline scenarios."""
    
    def test_insert_update_delete_sequence(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test a realistic INSERT → UPDATE → DELETE sequence."""
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic)
        
        customer_id = 1001
        
        # INSERT
        kafka_helper.produce_cdc_event(
            topic=topic,
            operation="INSERT",
            table="customers",
            data={"id": customer_id, "name": "New Customer", "status": "active"},
        )
        
        # UPDATE
        kafka_helper.produce_cdc_event(
            topic=topic,
            operation="UPDATE",
            table="customers",
            data={"id": customer_id, "name": "Updated Customer", "status": "active"},
        )
        
        # DELETE
        kafka_helper.produce_cdc_event(
            topic=topic,
            operation="DELETE",
            table="customers",
            data={"id": customer_id, "name": "Updated Customer", "status": "active"},
        )
        
        # Consume all events
        messages = kafka_helper.consume_messages(topic, count=3, timeout=15)
        
        assert len(messages) == 3
        
        operations = [m["operation"] for m in messages]
        assert operations == ["INSERT", "UPDATE", "DELETE"]
    
    def test_mixed_tables_processing(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test processing events from multiple tables."""
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic)
        
        # Events from different tables
        kafka_helper.produce_cdc_event(topic=topic, table="customers", data={"id": 1})
        kafka_helper.produce_cdc_event(topic=topic, table="orders", data={"id": 1})
        kafka_helper.produce_cdc_event(topic=topic, table="products", data={"id": 1})
        kafka_helper.produce_cdc_event(topic=topic, table="customers", data={"id": 2})
        
        # Consume all
        messages = kafka_helper.consume_messages(topic, count=4, timeout=15)
        
        # Verify table distribution
        tables = [m["source"]["table"] for m in messages]
        assert tables.count("customers") == 2
        assert tables.count("orders") == 1
        assert tables.count("products") == 1
    
    def test_high_volume_processing(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test processing a high volume of events."""
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic, num_partitions=3)
        
        num_events = 100
        
        # Produce many events
        start_time = time.time()
        for i in range(num_events):
            kafka_helper.produce_cdc_event(
                topic=topic,
                operation="INSERT",
                table="customers",
                data={"id": i, "name": f"Customer {i}"},
            )
        produce_time = time.time() - start_time
        
        # Consume all
        start_time = time.time()
        messages = kafka_helper.consume_messages(topic, count=num_events, timeout=30)
        consume_time = time.time() - start_time
        
        assert len(messages) == num_events
        
        # Log performance
        print(f"\nPerformance: {num_events} events")
        print(f"  Produce time: {produce_time:.2f}s ({num_events/produce_time:.0f} events/s)")
        print(f"  Consume time: {consume_time:.2f}s ({num_events/consume_time:.0f} events/s)")
    
    def test_duplicate_event_ids(
        self,
        kafka_helper,
        unique_test_id,
    ):
        """Test behavior with duplicate event IDs (deduplication)."""
        topic = f"test.cdc.{unique_test_id}"
        kafka_helper.create_topic(topic)
        
        # Same event_id sent twice
        event_id = str(uuid.uuid4())
        
        kafka_helper.produce_cdc_event(
            topic=topic,
            event_id=event_id,
            table="customers",
            data={"id": 1, "version": 1},
        )
        
        kafka_helper.produce_cdc_event(
            topic=topic,
            event_id=event_id,  # Same ID
            table="customers",
            data={"id": 1, "version": 2},
        )
        
        # Both messages exist in Kafka
        messages = kafka_helper.consume_messages(topic, count=2, timeout=10)
        
        assert len(messages) == 2
        assert messages[0]["event_id"] == event_id
        assert messages[1]["event_id"] == event_id
        
        # Note: Deduplication happens at consumer level, not Kafka level
