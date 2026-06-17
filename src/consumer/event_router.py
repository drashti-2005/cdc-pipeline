"""
Event Router - Multi-Sink Dispatcher
=====================================
Routes CDC events to configured sinks (MinIO, PostgreSQL, etc.)

WHAT IT DOES
------------
1. Receives CDC events from the Kafka consumer
2. Checks for duplicates (deduplication)
3. Routes to enabled sinks:
   - MinIO: Archive to Bronze layer (data lake)
   - PostgreSQL: Replicate to target database
4. Handles failures with retry logic
5. Sends failed events to Dead Letter Queue

ARCHITECTURE PATTERN: Fan-Out
-----------------------------
                    ┌──────────────┐
                    │  Event       │
                    │  Router      │
                    └──────┬───────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │  MinIO   │    │ PostgreSQL│   │   DLQ    │
    │  (Bronze)│    │  (Target) │   │ (Errors) │
    └──────────┘    └──────────┘    └──────────┘

WHY FAN-OUT?
------------
1. Single read from Kafka, multiple writes
2. Sinks are independent (failure in one doesn't block others)
3. Easy to add new sinks (just register in config)
4. Testable: mock sinks for unit tests
"""

import logging
import time
from typing import Optional

from confluent_kafka import Producer

from src.consumer import config
from src.consumer.deduplication import DeduplicationCache
from src.consumer.minio_sink import MinIOSink
from src.consumer.postgres_sink import PostgresSink
from src.schemas.cdc_event import CDCEvent, DLQEvent

logger = logging.getLogger(__name__)


class EventRouter:
    """
    Routes CDC events to multiple sinks with retry and error handling.

    Features:
    - Deduplication (skip already-processed events)
    - Multi-sink dispatch (MinIO + PostgreSQL)
    - Retry with exponential backoff
    - Dead Letter Queue for failed events
    - Graceful shutdown (flush all sinks)

    SIMPLE EXPLANATION:
    Think of this as a mail room in a large company:
    - Mail comes in (CDC events from Kafka)
    - Check if we've already handled this (deduplication)
    - Make copies and send to different departments (sinks)
    - If delivery fails, put in the problem pile (DLQ)
    """

    def __init__(self):
        """Initialize sinks based on configuration."""
        self._sinks_enabled = config.ENABLED_SINKS

        # Initialize deduplication cache
        if config.ENABLE_DEDUPLICATION:
            self._dedup = DeduplicationCache(max_size=config.DEDUP_CACHE_SIZE)
        else:
            self._dedup = None

        # Initialize sinks
        self._minio_sink: Optional[MinIOSink] = None
        self._postgres_sink: Optional[PostgresSink] = None

        if "minio" in self._sinks_enabled:
            self._minio_sink = MinIOSink()
            logger.info("MinIO sink enabled")

        if "postgres" in self._sinks_enabled:
            self._postgres_sink = PostgresSink()
            logger.info("PostgreSQL sink enabled")

        # DLQ producer for failed events
        self._dlq_producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        })

        # Metrics
        self._events_processed = 0
        self._events_failed = 0
        self._events_deduplicated = 0

        logger.info(f"Event router initialized with sinks: {self._sinks_enabled}")

    def route(self, event: CDCEvent) -> bool:
        """
        Route a single CDC event to all enabled sinks.

        Args:
            event: The CDC event to process

        Returns:
            True if event was processed successfully by all sinks
            False if event was sent to DLQ

        Processing flow:
        1. Check deduplication → skip if duplicate
        2. Write to MinIO → archive raw event
        3. Write to PostgreSQL → replicate change
        4. If any sink fails → retry or DLQ
        """
        # Step 1: Deduplication check
        if self._dedup and self._dedup.is_duplicate(event.event_id):
            self._events_deduplicated += 1
            logger.debug(f"Skipping duplicate event: {event.event_id}")
            return True

        # Step 2: Route to each sink with retry
        all_succeeded = True

        # MinIO sink (Bronze layer archive)
        if self._minio_sink:
            if not self._write_with_retry(self._minio_sink, event, "MinIO"):
                all_succeeded = False

        # PostgreSQL sink (target replication)
        if self._postgres_sink:
            if not self._write_with_retry(self._postgres_sink, event, "PostgreSQL"):
                all_succeeded = False

        # Update metrics
        if all_succeeded:
            self._events_processed += 1
        else:
            self._events_failed += 1
            self._send_to_dlq(event, "One or more sinks failed")

        return all_succeeded

    def _write_with_retry(self, sink, event: CDCEvent, sink_name: str) -> bool:
        """
        Attempt to write to a sink with exponential backoff retry.

        Args:
            sink: The sink to write to (MinIO or PostgreSQL)
            event: The CDC event to write
            sink_name: Name for logging

        Returns:
            True if write succeeded, False if all retries exhausted
        """
        last_error = None

        for attempt in range(config.MAX_RETRIES):
            try:
                sink.write(event)
                return True
            except Exception as e:
                last_error = e
                backoff = config.RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    f"{sink_name} write failed (attempt {attempt + 1}/{config.MAX_RETRIES}): {e}"
                )
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(backoff)

        logger.error(f"{sink_name} write failed after {config.MAX_RETRIES} attempts: {last_error}")
        return False

    def _send_to_dlq(self, event: CDCEvent, error_message: str) -> None:
        """
        Send a failed event to the Dead Letter Queue.

        DLQ events are wrapped with error metadata so operators
        can investigate and manually replay if needed.
        """
        dlq_event = DLQEvent(
            original_event=event,
            error_message=error_message,
            error_type="SinkFailure",
            failed_sink="multiple",
            retry_count=config.MAX_RETRIES,
        )

        try:
            self._dlq_producer.produce(
                topic=config.DLQ_TOPIC,
                key=event.get_partition_key().encode("utf-8"),
                value=dlq_event.model_dump_json().encode("utf-8"),
            )
            self._dlq_producer.flush()
            logger.info(f"Event sent to DLQ: {event.event_id}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")

    def flush_all(self) -> None:
        """Flush all sinks to ensure data is persisted."""
        if self._minio_sink:
            self._minio_sink.flush_all()
        if self._postgres_sink:
            self._postgres_sink.flush_all()
        self._dlq_producer.flush()

    def close(self) -> None:
        """Shutdown all sinks gracefully."""
        logger.info("Shutting down event router...")
        self.flush_all()

        if self._minio_sink:
            self._minio_sink.close()
        if self._postgres_sink:
            self._postgres_sink.close()

        logger.info("Event router shutdown complete")

    # ========================================================
    # Metrics (for monitoring)
    # ========================================================

    def get_stats(self) -> dict:
        """Get router statistics for monitoring."""
        stats = {
            "events_processed": self._events_processed,
            "events_failed": self._events_failed,
            "events_deduplicated": self._events_deduplicated,
        }
        if self._dedup:
            stats["dedup_cache"] = self._dedup.get_stats()
        if self._minio_sink:
            stats["minio_buffers"] = self._minio_sink.get_buffer_sizes()
        return stats
