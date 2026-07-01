"""
Unit Tests for DLQ Handler and Event Processor
===============================================
"""

import json
import uuid
import pytest
from unittest.mock import Mock, patch, MagicMock

from consumer.dlq_handler import (
    DLQHandler,
    DLQEntry,
    FailureReason,
)
from consumer.event_processor import (
    QualityAwareProcessor,
    ProcessingResult,
)
from quality import QualityChecker, RequiredFieldRule


class TestFailureReason:
    """Tests for FailureReason enum."""
    
    def test_all_reasons_have_values(self):
        """All failure reasons have string values."""
        for reason in FailureReason:
            assert isinstance(reason.value, str)
            assert len(reason.value) > 0
    
    def test_expected_reasons_exist(self):
        """Expected failure reasons are defined."""
        expected = [
            "quality_failure",
            "sink_failure",
            "deser_error",
            "schema_mismatch",
        ]
        values = [r.value for r in FailureReason]
        for e in expected:
            assert e in values


class TestDLQEntry:
    """Tests for DLQEntry dataclass."""
    
    def test_create_entry(self):
        """Create a DLQ entry."""
        entry = DLQEntry(
            original_event={"id": 123},
            original_topic="test-topic",
            original_partition=0,
            original_offset=100,
            failure_reason=FailureReason.QUALITY_FAILURE,
            error_message="Test error",
        )
        
        assert entry.original_event == {"id": 123}
        assert entry.failure_reason == FailureReason.QUALITY_FAILURE
        assert entry.error_message == "Test error"
    
    def test_to_dict(self):
        """Convert entry to dictionary."""
        entry = DLQEntry(
            original_event={"id": 123},
            original_topic="test-topic",
            original_partition=0,
            original_offset=100,
            failure_reason=FailureReason.SINK_FAILURE,
            error_message="DB connection failed",
            error_details={"attempts": 3},
        )
        
        d = entry.to_dict()
        
        assert d["original_event"] == {"id": 123}
        assert d["original_topic"] == "test-topic"
        assert d["failure_reason"] == "sink_failure"
        assert d["error_details"]["attempts"] == 3
    
    def test_failed_at_auto_set(self):
        """failed_at is automatically set to current time."""
        entry = DLQEntry(
            original_event={},
            original_topic="",
            original_partition=0,
            original_offset=0,
            failure_reason=FailureReason.UNKNOWN,
            error_message="",
        )
        
        assert entry.failed_at is not None
        assert "T" in entry.failed_at  # ISO format


class TestDLQHandler:
    """Tests for DLQHandler."""
    
    @pytest.fixture
    def mock_producer(self):
        """Create a mock Kafka producer."""
        with patch("src.consumer.dlq_handler.Producer") as mock:
            producer_instance = MagicMock()
            mock.return_value = producer_instance
            yield producer_instance
    
    @pytest.fixture
    def dlq_handler(self, mock_producer):
        """Create a DLQ handler with mocked producer."""
        handler = DLQHandler(
            dlq_topic="test-dlq",
            kafka_servers="localhost:9092",
            consumer_id="test-consumer",
        )
        return handler
    
    def test_init(self, dlq_handler):
        """Handler initializes correctly."""
        assert dlq_handler._dlq_topic == "test-dlq"
        assert dlq_handler._consumer_id == "test-consumer"
        assert dlq_handler._total_sent == 0
    
    def test_send_quality_failure(self, dlq_handler, mock_producer):
        """Send a quality failure to DLQ."""
        event = {"event_id": "test-123", "data": "test"}
        report = {
            "passed": False,
            "failure_details": [
                {"message": "Field 'name' is required"},
            ],
        }
        
        dlq_handler.send_quality_failure(
            event=event,
            quality_report=report,
            topic="source-topic",
            partition=1,
            offset=500,
        )
        
        # Verify producer was called
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        
        assert call_args.kwargs["topic"] == "test-dlq"
        
        # Verify message content
        value = json.loads(call_args.kwargs["value"].decode())
        assert value["failure_reason"] == "quality_failure"
        assert value["original_event"] == event
        assert value["quality_report"] == report
    
    def test_send_sink_failure(self, dlq_handler, mock_producer):
        """Send a sink failure to DLQ."""
        event = {"event_id": "test-456"}
        error = ConnectionError("Database unavailable")
        
        dlq_handler.send_sink_failure(
            event=event,
            sink_name="postgres",
            error=error,
            retry_count=3,
            topic="source-topic",
            partition=0,
            offset=100,
        )
        
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        
        value = json.loads(call_args.kwargs["value"].decode())
        assert value["failure_reason"] == "sink_failure"
        assert value["error_details"]["sink"] == "postgres"
        assert value["error_details"]["error_type"] == "ConnectionError"
        assert value["retry_count"] == 3
    
    def test_send_deserialization_error(self, dlq_handler, mock_producer):
        """Send a deserialization error to DLQ."""
        raw_message = b"invalid json {{{{"
        error = json.JSONDecodeError("Invalid", "", 0)
        
        dlq_handler.send_deserialization_error(
            raw_message=raw_message,
            error=error,
            topic="source-topic",
            partition=2,
            offset=999,
        )
        
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        
        value = json.loads(call_args.kwargs["value"].decode())
        assert value["failure_reason"] == "deser_error"
        assert "invalid json" in value["original_event"]["raw_message"]
    
    def test_get_stats(self, dlq_handler, mock_producer):
        """Get DLQ statistics."""
        # Send a few messages
        dlq_handler.send_quality_failure({}, {}, "", 0, 0)
        dlq_handler.send_quality_failure({}, {}, "", 0, 0)
        dlq_handler.send_sink_failure({}, "minio", Exception(), 0, "", 0, 0)
        
        stats = dlq_handler.get_stats()
        
        assert stats["total_sent"] == 3
        assert stats["by_reason"]["quality_failure"] == 2
        assert stats["by_reason"]["sink_failure"] == 1


class TestQualityAwareProcessor:
    """Tests for QualityAwareProcessor."""
    
    @pytest.fixture
    def mock_router(self):
        """Create a mock event router."""
        router = MagicMock()
        router.route.return_value = True
        router.get_stats.return_value = {}
        return router
    
    @pytest.fixture
    def mock_dlq(self):
        """Create a mock DLQ handler."""
        dlq = MagicMock()
        dlq.get_stats.return_value = {}
        return dlq
    
    @pytest.fixture
    def processor(self, mock_router, mock_dlq):
        """Create a processor with mocks."""
        return QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
            enable_quality_checks=True,
        )
    
    def test_process_valid_event(self, processor, mock_router):
        """Valid event passes through all stages."""
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INSERT",
            "source": {
                "database": "test_db",
                "schema": "public",
                "table": "customers",
            },
            "before": None,
            "after": {"id": 1, "name": "Test"},
        }
        raw_value = json.dumps(event).encode()
        
        result = processor.process_message(
            raw_value=raw_value,
            topic="test-topic",
            partition=0,
            offset=100,
        )
        
        assert result.success is True
        assert result.stage == "complete"
        mock_router.route.assert_called_once()
    
    def test_process_invalid_json(self, processor, mock_dlq):
        """Invalid JSON triggers deserialization error."""
        raw_value = b"not valid json {{{"
        
        result = processor.process_message(raw_value)
        
        assert result.success is False
        assert result.stage == "deserialization"
        mock_dlq.send_deserialization_error.assert_called_once()
    
    def test_process_quality_failure(self, processor, mock_dlq):
        """Event failing quality checks goes to DLQ."""
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INVALID_OP",  # Invalid operation
            "source": {
                "database": "test_db",
                "schema": "public",
                "table": "customers",
            },
        }
        raw_value = json.dumps(event).encode()
        
        result = processor.process_message(raw_value)
        
        assert result.success is False
        assert result.stage == "quality"
        mock_dlq.send_quality_failure.assert_called_once()
    
    def test_quality_checks_disabled(self, mock_router, mock_dlq):
        """Quality checks can be disabled."""
        processor = QualityAwareProcessor(
            router=mock_router,
            dlq_handler=mock_dlq,
            enable_quality_checks=False,
        )
        
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INVALID_OP",  # Would fail quality
            "source": {"database": "db", "schema": "s", "table": "t"},
            "before": None,
            "after": {"id": 1},
        }
        raw_value = json.dumps(event).encode()
        
        result = processor.process_message(raw_value)
        
        # Quality check skipped, routing attempted
        mock_dlq.send_quality_failure.assert_not_called()
    
    def test_register_custom_checker(self, processor):
        """Register a custom checker for a table."""
        checker = QualityChecker(name="custom")
        checker.add_rule(RequiredFieldRule("custom_field"))
        
        processor.register_checker("custom_table", checker)
        
        assert "custom_table" in processor._quality_checkers
    
    def test_get_stats(self, processor):
        """Get processor statistics."""
        # Process some events
        valid_event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INSERT",
            "source": {"database": "db", "schema": "s", "table": "t"},
            "before": None,
            "after": {"id": 1},
        }
        processor.process_message(json.dumps(valid_event).encode())
        
        stats = processor.get_stats()
        
        assert "processed" in stats
        assert "quality_passed" in stats
        assert "quality_pass_rate" in stats


class TestProcessingResult:
    """Tests for ProcessingResult dataclass."""
    
    def test_success_result(self):
        """Create a success result."""
        result = ProcessingResult(
            success=True,
            event_id="evt-123",
            stage="complete",
        )
        
        assert result.success is True
        assert result.event_id == "evt-123"
        assert result.error_message == ""
    
    def test_failure_result(self):
        """Create a failure result."""
        result = ProcessingResult(
            success=False,
            event_id="evt-456",
            stage="quality",
            error_message="Validation failed",
        )
        
        assert result.success is False
        assert result.stage == "quality"
        assert "Validation" in result.error_message
