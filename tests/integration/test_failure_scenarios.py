"""
Integration Tests for Failure Scenarios
========================================
Tests error handling, retries, and recovery.

Test Categories:
1. Quality check failures
2. Sink failures
3. Kafka failures
4. Recovery scenarios

Run with: pytest tests/integration/test_failure_scenarios.py -v -m integration
"""

import json
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from tests.integration.test_utils import (
    create_test_cdc_event,
    create_invalid_cdc_event,
)


# ============================================================
# Quality Check Failure Tests
# ============================================================

@pytest.mark.integration
class TestQualityCheckFailures:
    """Tests for various quality check failure scenarios."""
    
    def test_missing_required_field(self):
        """Test failure when required field is missing."""
        from consumer.event_processor import QualityAwareProcessor
        from consumer.dlq_handler import DLQHandler
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Event missing 'operation' field
        event = create_test_cdc_event()
        del event["operation"]
        
        result = processor.process_message(json.dumps(event).encode())
        
        # Should fail quality check
        assert result.success is False
        mock_dlq.send_quality_failure.assert_called()
    
    def test_invalid_operation_type(self):
        """Test failure with invalid operation type."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        event = create_test_cdc_event()
        event["operation"] = "TRUNCATE"  # Not a valid CDC operation
        
        result = processor.process_message(json.dumps(event).encode())
        
        assert result.success is False
        assert "quality" in result.stage.lower()
    
    def test_insert_without_after_data(self):
        """Test INSERT event without 'after' data."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        event = create_test_cdc_event(operation="INSERT")
        event["after"] = None  # INSERT must have after
        
        result = processor.process_message(json.dumps(event).encode())
        
        assert result.success is False
    
    def test_multiple_validation_failures(self):
        """Test event with multiple validation failures."""
        from quality import QualityChecker, RequiredFieldRule, TypeRule
        
        checker = (
            QualityChecker(name="test")
            .add_rule(RequiredFieldRule("id"))
            .add_rule(RequiredFieldRule("name"))
            .add_rule(TypeRule("age", int))
        )
        
        # Event with multiple failures
        data = {"age": "not-an-int"}  # Missing id, name; wrong type age
        
        report = checker.check(data)
        
        assert report.passed is False
        assert len(report.failures) >= 2  # At least 2 failures


# ============================================================
# Deserialization Failure Tests
# ============================================================

@pytest.mark.integration
class TestDeserializationFailures:
    """Tests for message parsing failures."""
    
    def test_invalid_json(self):
        """Test handling of invalid JSON."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        result = processor.process_message(b"{invalid json")
        
        assert result.success is False
        assert result.stage == "deserialization"
        mock_dlq.send_deserialization_error.assert_called_once()
    
    def test_empty_message(self):
        """Test handling of empty message."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        result = processor.process_message(b"")
        
        assert result.success is False
        assert result.stage == "deserialization"
    
    def test_binary_garbage(self):
        """Test handling of binary garbage data."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Random binary data
        garbage = bytes([0x00, 0xFF, 0x80, 0x7F, 0xAB, 0xCD])
        
        result = processor.process_message(garbage)
        
        assert result.success is False
        assert result.stage == "deserialization"
    
    def test_valid_json_invalid_schema(self):
        """Test valid JSON but wrong schema."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.get_stats.return_value = {}
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Valid JSON but completely wrong structure
        wrong_schema = {"foo": "bar", "baz": 123}
        
        result = processor.process_message(json.dumps(wrong_schema).encode())
        
        assert result.success is False


# ============================================================
# Sink Failure Tests
# ============================================================

@pytest.mark.integration
class TestSinkFailures:
    """Tests for sink write failures."""
    
    def test_router_exception_goes_to_dlq(self):
        """Test that router exceptions are sent to DLQ."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.side_effect = Exception("Sink connection failed")
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
            enable_quality_checks=False,  # Skip quality for this test
        )
        
        event = create_test_cdc_event()
        
        result = processor.process_message(json.dumps(event).encode())
        
        assert result.success is False
        assert result.stage == "routing"
        mock_dlq.send_sink_failure.assert_called_once()
    
    def test_router_returns_false_is_failure(self):
        """Test that router returning False is treated as failure."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.return_value = False  # Sink failed
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
            enable_quality_checks=False,
        )
        
        event = create_test_cdc_event()
        
        result = processor.process_message(json.dumps(event).encode())
        
        assert result.success is False
        assert result.stage == "routing"


# ============================================================
# Recovery Scenario Tests
# ============================================================

@pytest.mark.integration
class TestRecoveryScenarios:
    """Tests for pipeline recovery scenarios."""
    
    def test_continue_after_quality_failure(self):
        """Test that pipeline continues processing after quality failure."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.return_value = True
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Process invalid event
        invalid_event = create_test_cdc_event()
        invalid_event["operation"] = "INVALID"
        processor.process_message(json.dumps(invalid_event).encode())
        
        # Should still process valid event
        valid_event = create_test_cdc_event()
        result = processor.process_message(json.dumps(valid_event).encode())
        
        assert result.success is True
        mock_router.route.assert_called_once()  # Only valid event routed
    
    def test_stats_track_failures(self):
        """Test that statistics track failures correctly."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.return_value = True
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {"total_sent": 0, "by_reason": {}}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Process mix of valid and invalid events
        for i in range(5):
            valid_event = create_test_cdc_event()
            processor.process_message(json.dumps(valid_event).encode())
        
        for i in range(3):
            invalid_event = create_test_cdc_event()
            invalid_event["operation"] = "INVALID"
            processor.process_message(json.dumps(invalid_event).encode())
        
        stats = processor.get_stats()
        
        assert stats["processed"] == 8
        assert stats["quality_passed"] == 5
        assert stats["quality_failed"] == 3
        assert "62.5%" in stats["quality_pass_rate"]  # 5/8 = 62.5%


# ============================================================
# DLQ Structure Tests
# ============================================================

@pytest.mark.integration
class TestDLQMessageStructure:
    """Tests for DLQ message structure and content."""
    
    def test_dlq_entry_has_required_fields(self, kafka_helper, unique_test_id):
        """Test that DLQ entries have all required fields."""
        from consumer.dlq_handler import DLQHandler
        
        dlq_topic = f"test.dlq.{unique_test_id}"
        kafka_helper.create_topic(dlq_topic)
        
        dlq = DLQHandler(dlq_topic=dlq_topic, kafka_servers="127.0.0.1:9092")
        
        dlq.send_quality_failure(
            event={"test": "data"},
            quality_report={"passed": False},
            topic="source",
            partition=1,
            offset=100,
        )
        dlq.flush()
        
        messages = kafka_helper.consume_messages(dlq_topic, count=1, timeout=10)
        entry = messages[0]
        
        # Required fields
        assert "original_event" in entry
        assert "original_topic" in entry
        assert "original_partition" in entry
        assert "original_offset" in entry
        assert "failure_reason" in entry
        assert "error_message" in entry
        assert "failed_at" in entry
    
    def test_dlq_timestamps_are_iso_format(self, kafka_helper, unique_test_id):
        """Test that timestamps are in ISO format."""
        from consumer.dlq_handler import DLQHandler
        from datetime import datetime
        
        dlq_topic = f"test.dlq.{unique_test_id}"
        kafka_helper.create_topic(dlq_topic)
        
        dlq = DLQHandler(dlq_topic=dlq_topic, kafka_servers="127.0.0.1:9092")
        
        dlq.send_quality_failure(event={}, quality_report={}, topic="", partition=0, offset=0)
        dlq.flush()
        
        messages = kafka_helper.consume_messages(dlq_topic, count=1, timeout=10)
        entry = messages[0]
        
        # Should be parseable as ISO timestamp
        failed_at = entry["failed_at"]
        parsed = datetime.fromisoformat(failed_at.replace("Z", "+00:00"))
        assert parsed is not None
    
    def test_dlq_preserves_original_event_exactly(self, kafka_helper, unique_test_id):
        """Test that original event is preserved exactly."""
        from consumer.dlq_handler import DLQHandler
        
        dlq_topic = f"test.dlq.{unique_test_id}"
        kafka_helper.create_topic(dlq_topic)
        
        dlq = DLQHandler(dlq_topic=dlq_topic, kafka_servers="127.0.0.1:9092")
        
        original = {
            "event_id": "test-123",
            "complex_data": {
                "nested": {"deep": "value"},
                "list": [1, 2, 3],
            },
            "unicode": "日本語テスト",
        }
        
        dlq.send_quality_failure(event=original, quality_report={}, topic="", partition=0, offset=0)
        dlq.flush()
        
        messages = kafka_helper.consume_messages(dlq_topic, count=1, timeout=10)
        
        assert messages[0]["original_event"] == original


# ============================================================
# Stress/Edge Case Tests
# ============================================================

@pytest.mark.integration
class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""
    
    def test_very_large_event(self):
        """Test handling of large events."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.return_value = True
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        # Create large event (1MB of data)
        large_data = {"field_" + str(i): "x" * 1000 for i in range(1000)}
        event = create_test_cdc_event(data=large_data)
        
        result = processor.process_message(json.dumps(event).encode())
        
        # Should process successfully (may pass or fail quality based on rules)
        # The point is it shouldn't crash
        assert result is not None
    
    def test_unicode_in_events(self):
        """Test handling of unicode characters."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.return_value = True
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        unicode_data = {
            "name": "日本語テスト",
            "emoji": "🚀📊💾",
            "russian": "Тест данных",
            "arabic": "اختبار البيانات",
        }
        event = create_test_cdc_event(data=unicode_data)
        
        result = processor.process_message(json.dumps(event).encode())
        
        assert result.success is True
    
    def test_null_values_in_event(self):
        """Test handling of null values."""
        from consumer.event_processor import QualityAwareProcessor
        
        mock_router = MagicMock()
        mock_router.route.return_value = True
        mock_router.get_stats.return_value = {}
        
        mock_dlq = MagicMock()
        mock_dlq.get_stats.return_value = {}
        
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
        )
        
        event = create_test_cdc_event()
        event["after"]["nullable_field"] = None
        event["before"] = None  # Before is null for INSERT
        
        result = processor.process_message(json.dumps(event).encode())
        
        assert result.success is True
