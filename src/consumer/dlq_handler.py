"""
Dead Letter Queue (DLQ) Handler
================================
Handles events that fail processing - quality failures, sink errors, etc.

WHAT IS A DLQ?
--------------
A Dead Letter Queue is a special queue where problematic messages go when
they can't be processed successfully. It's like the "return to sender"
pile at a post office.

WHY DO WE NEED IT?
------------------
1. **Don't lose data**: Instead of dropping bad events, we save them
2. **Debugging**: Engineers can inspect why events failed
3. **Replay**: After fixing issues, events can be replayed
4. **Monitoring**: Track failure patterns and alert on spikes

DLQ FLOW
--------
    CDC Event
        │
        ▼
    ┌─────────────────┐
    │ Quality Check   │──────── PASS ──────▶ Process normally
    └─────────────────┘
        │
        ▼ FAIL
    ┌─────────────────┐
    │ DLQ Handler     │──────▶ Kafka DLQ Topic
    └─────────────────┘
        │
        ▼
    ┌─────────────────┐
    │ DLQ Consumer    │──────▶ Alert / Review / Replay
    └─────────────────┘
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from confluent_kafka import Producer

from . import config
from metrics import (
    DLQ_EVENTS_TOTAL,
    DLQ_EVENTS_BY_REASON,
)

logger = logging.getLogger(__name__)


class FailureReason(Enum):
    """
    Categorizes why an event failed processing.
    
    SIMPLE EXPLANATION:
    Different types of problems need different fixes:
    - Quality issues = fix source data or adjust rules
    - Sink failures = fix infrastructure
    - Deserialization = fix schema or producer
    """
    QUALITY_FAILURE = "quality_failure"        # Failed data quality checks
    SINK_FAILURE = "sink_failure"              # Database/MinIO write failed
    DESERIALIZATION_ERROR = "deser_error"      # Couldn't parse the message
    SCHEMA_MISMATCH = "schema_mismatch"        # Schema incompatibility
    TRANSFORMATION_ERROR = "transform_error"   # Error during data transformation
    TIMEOUT = "timeout"                        # Processing took too long
    UNKNOWN = "unknown"                        # Catch-all for unexpected errors


@dataclass
class DLQEntry:
    """
    A structured entry for the Dead Letter Queue.
    
    Contains:
    - The original event (for replay)
    - Why it failed (for debugging)
    - When it failed (for tracking)
    - Where it came from (for routing)
    """
    # Original event data
    original_event: dict
    original_topic: str
    original_partition: int
    original_offset: int
    
    # Failure information
    failure_reason: FailureReason
    error_message: str
    error_details: Optional[dict] = None
    
    # Metadata
    failed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    retry_count: int = 0
    consumer_id: str = ""
    
    # Quality-specific (if quality failure)
    quality_report: Optional[dict] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "original_event": self.original_event,
            "original_topic": self.original_topic,
            "original_partition": self.original_partition,
            "original_offset": self.original_offset,
            "failure_reason": self.failure_reason.value,
            "error_message": self.error_message,
            "error_details": self.error_details,
            "failed_at": self.failed_at,
            "retry_count": self.retry_count,
            "consumer_id": self.consumer_id,
            "quality_report": self.quality_report,
        }


class DLQHandler:
    """
    Handles routing of failed events to the Dead Letter Queue.
    
    SIMPLE EXPLANATION:
    Think of this as the "problem manager" in a factory:
    - When something goes wrong, report it here
    - It logs the problem, updates metrics, and saves the event
    - Later, someone can review and fix the issues
    
    TECHNICAL DETAILS:
    - Uses Kafka producer to write to DLQ topic
    - Records Prometheus metrics for monitoring
    - Supports different failure reasons for categorization
    - Can batch DLQ writes for efficiency (optional)
    """
    
    def __init__(
        self,
        dlq_topic: str = None,
        kafka_servers: str = None,
        consumer_id: str = "",
    ):
        """
        Initialize the DLQ handler.
        
        Args:
            dlq_topic: Kafka topic for DLQ messages
            kafka_servers: Kafka bootstrap servers
            consumer_id: Identifier for this consumer instance
        """
        self._dlq_topic = dlq_topic or config.DLQ_TOPIC
        self._consumer_id = consumer_id or f"consumer-{id(self)}"
        
        # Kafka producer for DLQ writes
        self._producer = Producer({
            "bootstrap.servers": kafka_servers or config.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",  # Ensure DLQ writes are durable
        })
        
        # Statistics
        self._total_sent = 0
        self._by_reason: dict[str, int] = {}
        
        logger.info(f"DLQ handler initialized, topic: {self._dlq_topic}")
    
    def send_quality_failure(
        self,
        event: dict,
        quality_report: dict,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        """
        Send an event that failed quality checks to the DLQ.
        
        Args:
            event: The original event data
            quality_report: The quality check results
            topic: Original Kafka topic
            partition: Original partition
            offset: Original offset
        """
        # Build error message from failures
        failures = quality_report.get("failure_details", [])
        failure_messages = [f.get("message", "Unknown") for f in failures]
        error_message = f"Quality checks failed: {', '.join(failure_messages[:3])}"
        if len(failures) > 3:
            error_message += f" (and {len(failures) - 3} more)"
        
        entry = DLQEntry(
            original_event=event,
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            failure_reason=FailureReason.QUALITY_FAILURE,
            error_message=error_message,
            error_details={"failure_count": len(failures)},
            consumer_id=self._consumer_id,
            quality_report=quality_report,
        )
        
        self._send(entry)
    
    def send_sink_failure(
        self,
        event: dict,
        sink_name: str,
        error: Exception,
        retry_count: int = 0,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        """
        Send an event that failed sink writes to the DLQ.
        
        Args:
            event: The original event data
            sink_name: Name of the failed sink (e.g., "minio", "postgres")
            error: The exception that caused the failure
            retry_count: Number of retry attempts made
            topic: Original Kafka topic
            partition: Original partition
            offset: Original offset
        """
        entry = DLQEntry(
            original_event=event,
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            failure_reason=FailureReason.SINK_FAILURE,
            error_message=f"{sink_name} write failed: {str(error)}",
            error_details={
                "sink": sink_name,
                "error_type": type(error).__name__,
                "error_str": str(error),
            },
            retry_count=retry_count,
            consumer_id=self._consumer_id,
        )
        
        self._send(entry)
    
    def send_deserialization_error(
        self,
        raw_message: bytes,
        error: Exception,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        """
        Send a message that couldn't be deserialized to the DLQ.
        
        Args:
            raw_message: The raw bytes that couldn't be parsed
            error: The parsing exception
            topic: Original Kafka topic
            partition: Original partition
            offset: Original offset
        """
        # Try to decode as string for logging
        try:
            raw_str = raw_message.decode("utf-8")[:500]  # Truncate for safety
        except:
            raw_str = f"<binary {len(raw_message)} bytes>"
        
        entry = DLQEntry(
            original_event={"raw_message": raw_str},
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            failure_reason=FailureReason.DESERIALIZATION_ERROR,
            error_message=f"Failed to deserialize: {str(error)}",
            error_details={
                "error_type": type(error).__name__,
                "raw_size_bytes": len(raw_message),
            },
            consumer_id=self._consumer_id,
        )
        
        self._send(entry)
    
    def send_generic_failure(
        self,
        event: dict,
        reason: FailureReason,
        error_message: str,
        error_details: dict = None,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        """
        Send a generic failure to the DLQ.
        
        Use this for failures that don't fit other categories.
        """
        entry = DLQEntry(
            original_event=event,
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            failure_reason=reason,
            error_message=error_message,
            error_details=error_details,
            consumer_id=self._consumer_id,
        )
        
        self._send(entry)
    
    def _send(self, entry: DLQEntry) -> None:
        """
        Send a DLQ entry to Kafka.
        
        Updates metrics and logs the failure.
        """
        import json
        
        reason = entry.failure_reason.value
        
        try:
            # Serialize entry
            value = json.dumps(entry.to_dict()).encode("utf-8")
            
            # Build key from original topic/partition for routing
            key = f"{entry.original_topic}:{entry.original_partition}".encode("utf-8")
            
            # Send to DLQ topic
            self._producer.produce(
                topic=self._dlq_topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            
            # Update metrics
            self._total_sent += 1
            self._by_reason[reason] = self._by_reason.get(reason, 0) + 1
            
            DLQ_EVENTS_TOTAL.inc()
            DLQ_EVENTS_BY_REASON.labels(reason=reason).inc()
            
            logger.warning(
                f"Event sent to DLQ | reason={reason} | "
                f"topic={entry.original_topic} | offset={entry.original_offset}"
            )
            
        except Exception as e:
            # DLQ send failed - this is serious, log it prominently
            logger.error(
                f"CRITICAL: Failed to send to DLQ | reason={reason} | "
                f"error={e} | event will be lost"
            )
    
    def _delivery_callback(self, err, msg) -> None:
        """Kafka delivery callback for DLQ messages."""
        if err:
            logger.error(f"DLQ delivery failed: {err}")
        else:
            logger.debug(f"DLQ message delivered: {msg.topic()}[{msg.partition()}]")
    
    def flush(self) -> None:
        """Flush pending DLQ messages to Kafka."""
        self._producer.flush()
    
    def close(self) -> None:
        """Close the DLQ handler."""
        self.flush()
        logger.info(f"DLQ handler closed, total sent: {self._total_sent}")
    
    def get_stats(self) -> dict:
        """Get DLQ statistics."""
        return {
            "total_sent": self._total_sent,
            "by_reason": dict(self._by_reason),
        }


# ============================================================
# Singleton Pattern for Shared DLQ Handler
# ============================================================

_dlq_handler: Optional[DLQHandler] = None


def get_dlq_handler(consumer_id: str = "") -> DLQHandler:
    """
    Get the singleton DLQ handler instance.
    
    Creates the handler on first call, returns existing on subsequent calls.
    """
    global _dlq_handler
    if _dlq_handler is None:
        _dlq_handler = DLQHandler(consumer_id=consumer_id)
    return _dlq_handler


def reset_dlq_handler() -> None:
    """Reset the DLQ handler (for testing)."""
    global _dlq_handler
    if _dlq_handler:
        _dlq_handler.close()
    _dlq_handler = None
