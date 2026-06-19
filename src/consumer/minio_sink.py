"""
MinIO Sink - Bronze Layer Archive
===================================
Archives raw CDC events to MinIO (S3-compatible object storage).

This is the "Bronze" layer in the medallion architecture:
- Raw, unprocessed events
- Partitioned by table/date/hour for efficient querying
- Immutable audit trail of all changes

WHY BRONZE LAYER?
-----------------
1. Replay: If processing fails, replay from Bronze
2. Audit: Complete history of all changes
3. Analytics: Feed Spark/Presto for historical analysis
4. Compliance: Regulatory requirement to keep raw data
5. ML: Training data for anomaly detection

FILE FORMAT
-----------
Events are batched into JSON Lines (.jsonl) files:
  - One JSON object per line
  - Efficient for streaming reads
  - Compatible with Spark, Pandas, DuckDB

PATH STRUCTURE
--------------
{bucket}/{table}/{year}/{month}/{day}/{hour}/events_{uuid}.jsonl
Example: cdc-bronze/orders/2024/01/15/14/events_abc123.jsonl
"""

import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from threading import Lock, Thread
from time import sleep, time
from typing import Optional

from minio import Minio
from minio.error import S3Error

from src.consumer import config
from src.schemas.cdc_event import CDCEvent
from src.metrics import record_minio_write, MINIO_BUFFER_SIZE

logger = logging.getLogger(__name__)


class MinIOSink:
    """
    Batches CDC events and writes them to MinIO as JSON Lines files.

    Features:
    - Time-based flushing (flush every N seconds)
    - Size-based flushing (flush every N events)
    - Per-table buffering (separate files per table)
    - Thread-safe operations

    SIMPLE EXPLANATION:
    Think of this like a mail sorting facility:
    - Events come in continuously
    - We group them by "destination" (table name)
    - Every few seconds (or when we have enough), we ship a batch
    """

    def __init__(self):
        """Initialize MinIO client and start background flush thread."""
        self.client = Minio(
            endpoint=config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_SECURE,
        )

        # Buffers: table_name -> list of events
        self._buffers: dict[str, list[CDCEvent]] = defaultdict(list)
        self._buffer_lock = Lock()

        # Track when each buffer was last flushed
        self._last_flush: dict[str, float] = defaultdict(time)

        # Ensure bucket exists
        self._ensure_bucket()

        # Start background flush thread
        self._running = True
        self._flush_thread = Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

        logger.info(f"MinIO sink initialized, bucket: {config.MINIO_BUCKET}")

    def _ensure_bucket(self):
        """Create the Bronze bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(config.MINIO_BUCKET):
                self.client.make_bucket(config.MINIO_BUCKET)
                logger.info(f"Created bucket: {config.MINIO_BUCKET}")
        except S3Error as e:
            logger.error(f"Failed to create bucket: {e}")
            raise

    def write(self, event: CDCEvent) -> None:
        """
        Add an event to the buffer for its table.

        The event will be written to MinIO when:
        1. Buffer reaches MINIO_BATCH_SIZE, OR
        2. MINIO_FLUSH_INTERVAL_S seconds have passed

        SIMPLE EXPLANATION:
        Like dropping a letter in the outbox - it'll be
        picked up and delivered with the next batch.
        """
        table = event.source.table

        with self._buffer_lock:
            self._buffers[table].append(event)
            
            # Update buffer size metric
            MINIO_BUFFER_SIZE.labels(table=table).set(len(self._buffers[table]))

            # Check if we should flush due to size
            if len(self._buffers[table]) >= config.MINIO_BATCH_SIZE:
                self._flush_table(table)

    def _flush_loop(self):
        """Background thread: flush buffers that haven't been flushed recently."""
        while self._running:
            sleep(5)  # Check every 5 seconds

            with self._buffer_lock:
                now = time()
                for table in list(self._buffers.keys()):
                    # Flush if we have events and it's been too long
                    if (
                        self._buffers[table]
                        and now - self._last_flush[table] > config.MINIO_FLUSH_INTERVAL_S
                    ):
                        self._flush_table(table)

    def _flush_table(self, table: str) -> None:
        """
        Write all buffered events for a table to MinIO.

        Creates a JSON Lines file with all events, uploads to MinIO,
        then clears the buffer.

        IMPORTANT: Called with _buffer_lock held!
        """
        events = self._buffers[table]
        if not events:
            return

        # Generate object path
        now = datetime.utcnow()
        path = self._build_path(table, now)

        # Serialize events to JSON Lines format
        lines = []
        for event in events:
            lines.append(event.model_dump_json())
        content = "\n".join(lines) + "\n"
        data = content.encode("utf-8")

        # Upload to MinIO
        try:
            start_time = time()
            self.client.put_object(
                bucket_name=config.MINIO_BUCKET,
                object_name=path,
                data=BytesIO(data),
                length=len(data),
                content_type="application/x-ndjson",
            )
            duration = time() - start_time
            event_count = len(events)
            
            # Record metrics
            record_minio_write(table, event_count, duration)
            
            logger.info(f"Wrote {event_count} events to s3://{config.MINIO_BUCKET}/{path}")

            # Clear buffer and update flush time
            self._buffers[table] = []
            self._last_flush[table] = time()
            
            # Update buffer size metric
            MINIO_BUFFER_SIZE.labels(table=table).set(0)

        except S3Error as e:
            logger.error(f"Failed to write to MinIO: {e}")
            # Keep events in buffer for retry
            raise

    def _build_path(self, table: str, ts: datetime) -> str:
        """
        Build the S3 object path using the configured pattern.

        Pattern: {table}/{year}/{month}/{day}/{hour}/events_{uuid}.jsonl
        Example: orders/2024/01/15/14/events_abc123.jsonl
        """
        base_path = config.MINIO_PATH_PATTERN.format(
            table=table,
            year=ts.year,
            month=ts.month,
            day=ts.day,
            hour=ts.hour,
        )
        filename = f"events_{uuid.uuid4().hex[:8]}.jsonl"
        return f"{base_path}/{filename}"

    def flush_all(self) -> None:
        """Force flush all buffers immediately."""
        with self._buffer_lock:
            for table in list(self._buffers.keys()):
                if self._buffers[table]:
                    self._flush_table(table)

    def close(self) -> None:
        """Shutdown: flush remaining events and stop background thread."""
        logger.info("Shutting down MinIO sink...")
        self._running = False
        self.flush_all()
        self._flush_thread.join(timeout=5)
        logger.info("MinIO sink shutdown complete")

    # ========================================================
    # Metrics (for monitoring in Phase 13)
    # ========================================================

    def get_buffer_sizes(self) -> dict[str, int]:
        """Return current buffer sizes per table."""
        with self._buffer_lock:
            return {table: len(events) for table, events in self._buffers.items()}
