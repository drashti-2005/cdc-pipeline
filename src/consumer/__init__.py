"""
CDC Consumer Package
====================
Reads CDC events from Kafka and routes them to multiple sinks.

Components:
- config.py: Consumer configuration (Kafka, MinIO, Target PostgreSQL)
- kafka_consumer.py: Main consumer process that polls Kafka
- event_router.py: Routes events to configured sinks
- minio_sink.py: Archives events to MinIO (Bronze layer)
- postgres_sink.py: Replicates changes to target PostgreSQL
- deduplication.py: LRU cache for exactly-once semantics
- dlq_handler.py: Dead Letter Queue for failed events

Usage:
    python -m src.consumer.kafka_consumer

Or programmatically:
    from src.consumer.kafka_consumer import CDCConsumer
    consumer = CDCConsumer()
    consumer.run()
"""

from src.consumer.kafka_consumer import CDCConsumer
from src.consumer.event_router import EventRouter
from src.consumer.minio_sink import MinIOSink
from src.consumer.postgres_sink import PostgresSink
from src.consumer.deduplication import DeduplicationCache
from src.consumer.dlq_handler import (
    DLQHandler,
    DLQEntry,
    FailureReason,
    get_dlq_handler,
)
from src.consumer.event_processor import (
    QualityAwareProcessor,
    ProcessingResult,
    create_processor_with_table_checkers,
)

__all__ = [
    "CDCConsumer",
    "EventRouter",
    "MinIOSink",
    "PostgresSink",
    "DeduplicationCache",
    "DLQHandler",
    "DLQEntry",
    "FailureReason",
    "get_dlq_handler",
    "QualityAwareProcessor",
    "ProcessingResult",
    "create_processor_with_table_checkers",
]
