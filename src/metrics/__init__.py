"""
CDC Pipeline Metrics Package
=============================
Prometheus metrics for monitoring pipeline health.

Usage:
    from src.metrics import start_metrics_server, record_producer_event
    
    # Start metrics server (once at startup)
    start_metrics_server(port=8000)
    
    # Record events
    record_producer_event("customers", "INSERT")
"""

from src.metrics.pipeline_metrics import (
    # Server
    start_metrics_server,
    
    # Producer helpers
    record_producer_event,
    record_kafka_publish,
    record_kafka_error,
    
    # Consumer helpers
    record_consumer_event,
    record_event_processed,
    record_event_failed,
    record_minio_write,
    record_postgres_write,
    
    # Raw metrics (for direct access)
    PRODUCER_EVENTS_TOTAL,
    PRODUCER_KAFKA_PUBLISHED,
    PRODUCER_WAL_LAG_BYTES,
    PRODUCER_TRANSACTION_DURATION,
    CONSUMER_EVENTS_TOTAL,
    CONSUMER_EVENTS_PROCESSED,
    CONSUMER_EVENTS_FAILED,
    CONSUMER_EVENTS_DEDUPLICATED,
    CONSUMER_PROCESSING_DURATION,
    CONSUMER_LAG,
    CONSUMER_OFFSET,
    MINIO_BUFFER_SIZE,
    POSTGRES_CONNECTION_ERRORS,
    DEDUP_CACHE_SIZE,
    DEDUP_CACHE_HITS,
    DEDUP_CACHE_MISSES,
    
    # Data Quality
    DATA_QUALITY_CHECKS_TOTAL,
    DATA_QUALITY_FAILURES_TOTAL,
    DATA_QUALITY_PASS_RATE,
    
    # Dead Letter Queue
    DLQ_EVENTS_TOTAL,
    DLQ_EVENTS_BY_REASON,
)

__all__ = [
    "start_metrics_server",
    "record_producer_event",
    "record_kafka_publish",
    "record_kafka_error",
    "record_consumer_event",
    "record_event_processed",
    "record_event_failed",
    "record_minio_write",
    "record_postgres_write",
    "DATA_QUALITY_CHECKS_TOTAL",
    "DATA_QUALITY_FAILURES_TOTAL",
    "DATA_QUALITY_PASS_RATE",
    "DLQ_EVENTS_TOTAL",
    "DLQ_EVENTS_BY_REASON",
]
