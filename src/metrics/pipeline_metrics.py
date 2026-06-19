"""
CDC Pipeline Metrics - Prometheus Instrumentation
===================================================
Exposes metrics for monitoring the CDC pipeline health and performance.

WHAT THIS DOES:
- Tracks events processed, errors, latency
- Exposes /metrics endpoint for Prometheus to scrape
- Enables alerting and dashboards in Grafana

METRIC TYPES:
- Counter: Cumulative values (total events processed)
- Gauge: Current values (buffer size, lag)
- Histogram: Distributions (latency percentiles)

SIMPLE EXPLANATION:
Think of this as health sensors in a factory:
- How many items processed? (Counter)
- How full is the queue? (Gauge)
- How long does each step take? (Histogram)
"""

import logging
import threading
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Info,
    start_http_server,
    REGISTRY,
)

logger = logging.getLogger(__name__)


# ============================================================
# Application Info
# ============================================================
APP_INFO = Info(
    "cdc_pipeline",
    "CDC Pipeline application information",
)
APP_INFO.info({
    "version": "1.0.0",
    "component": "cdc-pipeline",
})


# ============================================================
# Producer Metrics
# ============================================================

# Total events captured from WAL
PRODUCER_EVENTS_TOTAL = Counter(
    "cdc_producer_events_total",
    "Total number of CDC events captured from PostgreSQL WAL",
    ["table", "operation"],  # Labels: customers/orders, INSERT/UPDATE/DELETE
)

# Events published to Kafka
PRODUCER_KAFKA_PUBLISHED = Counter(
    "cdc_producer_kafka_published_total",
    "Total number of events successfully published to Kafka",
    ["topic"],
)

# Kafka publish errors
PRODUCER_KAFKA_ERRORS = Counter(
    "cdc_producer_kafka_errors_total",
    "Total number of Kafka publish errors",
    ["topic", "error_type"],
)

# Current WAL lag (bytes behind)
PRODUCER_WAL_LAG_BYTES = Gauge(
    "cdc_producer_wal_lag_bytes",
    "Current WAL lag in bytes (how far behind we are)",
)

# Current LSN position
PRODUCER_LSN_POSITION = Gauge(
    "cdc_producer_lsn_position",
    "Current LSN position being processed",
)

# Transaction processing time
PRODUCER_TRANSACTION_DURATION = Histogram(
    "cdc_producer_transaction_duration_seconds",
    "Time to process a complete transaction",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


# ============================================================
# Consumer Metrics
# ============================================================

# Events consumed from Kafka
CONSUMER_EVENTS_TOTAL = Counter(
    "cdc_consumer_events_total",
    "Total number of CDC events consumed from Kafka",
    ["topic"],
)

# Events processed successfully
CONSUMER_EVENTS_PROCESSED = Counter(
    "cdc_consumer_events_processed_total",
    "Total number of events successfully processed",
    ["sink"],  # minio, postgres
)

# Events failed (sent to DLQ)
CONSUMER_EVENTS_FAILED = Counter(
    "cdc_consumer_events_failed_total",
    "Total number of events that failed processing",
    ["sink", "error_type"],
)

# Events deduplicated (skipped)
CONSUMER_EVENTS_DEDUPLICATED = Counter(
    "cdc_consumer_events_deduplicated_total",
    "Total number of duplicate events skipped",
)

# Event processing latency
CONSUMER_PROCESSING_DURATION = Histogram(
    "cdc_consumer_processing_duration_seconds",
    "Time to process a single event through all sinks",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# Consumer lag (messages behind)
CONSUMER_LAG = Gauge(
    "cdc_consumer_lag_messages",
    "Number of messages behind the latest offset",
    ["topic", "partition"],
)

# Current offset position
CONSUMER_OFFSET = Gauge(
    "cdc_consumer_offset",
    "Current offset position",
    ["topic", "partition"],
)


# ============================================================
# MinIO Sink Metrics
# ============================================================

MINIO_WRITES_TOTAL = Counter(
    "cdc_minio_writes_total",
    "Total number of files written to MinIO",
)

MINIO_EVENTS_ARCHIVED = Counter(
    "cdc_minio_events_archived_total",
    "Total number of events archived to MinIO",
    ["table"],
)

MINIO_BUFFER_SIZE = Gauge(
    "cdc_minio_buffer_size",
    "Current number of events in MinIO buffer",
    ["table"],
)

MINIO_WRITE_DURATION = Histogram(
    "cdc_minio_write_duration_seconds",
    "Time to write a batch to MinIO",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)


# ============================================================
# PostgreSQL Sink Metrics
# ============================================================

POSTGRES_WRITES_TOTAL = Counter(
    "cdc_postgres_writes_total",
    "Total number of writes to target PostgreSQL",
    ["table", "operation"],
)

POSTGRES_WRITE_DURATION = Histogram(
    "cdc_postgres_write_duration_seconds",
    "Time to write a single event to PostgreSQL",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

POSTGRES_CONNECTION_ERRORS = Counter(
    "cdc_postgres_connection_errors_total",
    "Total number of PostgreSQL connection errors",
)


# ============================================================
# Deduplication Cache Metrics
# ============================================================

DEDUP_CACHE_SIZE = Gauge(
    "cdc_dedup_cache_size",
    "Current number of entries in deduplication cache",
)

DEDUP_CACHE_HITS = Counter(
    "cdc_dedup_cache_hits_total",
    "Total number of cache hits (duplicates detected)",
)

DEDUP_CACHE_MISSES = Counter(
    "cdc_dedup_cache_misses_total",
    "Total number of cache misses (new events)",
)


# ============================================================
# Data Quality Metrics
# ============================================================

DATA_QUALITY_CHECKS_TOTAL = Counter(
    "cdc_data_quality_checks_total",
    "Total number of data quality checks performed",
    ["checker", "rule"],
)

DATA_QUALITY_FAILURES_TOTAL = Counter(
    "cdc_data_quality_failures_total",
    "Total number of data quality check failures",
    ["checker", "rule", "severity"],
)

DATA_QUALITY_PASS_RATE = Gauge(
    "cdc_data_quality_pass_rate",
    "Percentage of events passing quality checks",
    ["checker"],
)


# ============================================================
# Dead Letter Queue Metrics
# ============================================================

DLQ_EVENTS_TOTAL = Counter(
    "cdc_dlq_events_total",
    "Total number of events sent to Dead Letter Queue",
)

DLQ_EVENTS_BY_REASON = Counter(
    "cdc_dlq_events_by_reason_total",
    "Events sent to DLQ by failure reason",
    ["reason"],
)


# ============================================================
# Metrics Server
# ============================================================

_metrics_server_started = False
_server_lock = threading.Lock()


def start_metrics_server(port: int = 8000) -> None:
    """
    Start the Prometheus metrics HTTP server.

    This exposes a /metrics endpoint that Prometheus scrapes.
    Call this once at application startup.
    """
    global _metrics_server_started

    with _server_lock:
        if _metrics_server_started:
            logger.warning("Metrics server already started")
            return

        try:
            start_http_server(port)
            _metrics_server_started = True
            logger.info(f"Prometheus metrics server started on port {port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            raise


# ============================================================
# Helper Functions
# ============================================================

def record_producer_event(table: str, operation: str) -> None:
    """Record a CDC event captured from WAL."""
    PRODUCER_EVENTS_TOTAL.labels(table=table, operation=operation).inc()


def record_kafka_publish(topic: str) -> None:
    """Record a successful Kafka publish."""
    PRODUCER_KAFKA_PUBLISHED.labels(topic=topic).inc()


def record_kafka_error(topic: str, error_type: str) -> None:
    """Record a Kafka publish error."""
    PRODUCER_KAFKA_ERRORS.labels(topic=topic, error_type=error_type).inc()


def record_consumer_event(topic: str) -> None:
    """Record an event consumed from Kafka."""
    CONSUMER_EVENTS_TOTAL.labels(topic=topic).inc()


def record_event_processed(sink: str) -> None:
    """Record a successfully processed event."""
    CONSUMER_EVENTS_PROCESSED.labels(sink=sink).inc()


def record_event_failed(sink: str, error_type: str) -> None:
    """Record a failed event."""
    CONSUMER_EVENTS_FAILED.labels(sink=sink, error_type=error_type).inc()


def record_minio_write(table: str, event_count: int, duration: float) -> None:
    """Record a MinIO write operation."""
    MINIO_WRITES_TOTAL.inc()
    MINIO_EVENTS_ARCHIVED.labels(table=table).inc(event_count)
    MINIO_WRITE_DURATION.observe(duration)


def record_postgres_write(table: str, operation: str, duration: float) -> None:
    """Record a PostgreSQL write operation."""
    POSTGRES_WRITES_TOTAL.labels(table=table, operation=operation).inc()
    POSTGRES_WRITE_DURATION.observe(duration)

