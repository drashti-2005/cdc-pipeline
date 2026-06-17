"""
CDC Consumer - Configuration
==============================
All configuration for the CDC consumer, loaded from environment variables.
Includes settings for Kafka consumer, MinIO (Bronze layer), and target PostgreSQL.
"""

import os


# ============================================================
# Kafka Consumer
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Consumer group ID - allows scaling out with multiple consumers
CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "cdc-consumer-group")

# Topics to subscribe to (comma-separated)
# Default: all tables we're capturing
KAFKA_TOPICS = os.getenv(
    "KAFKA_TOPICS",
    "cdc.source.public.customers,cdc.source.public.products,cdc.source.public.orders,cdc.source.public.order_items",
).split(",")

# Auto offset reset: 'earliest' to replay from beginning, 'latest' for new only
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# How long to wait for new messages (milliseconds)
CONSUMER_POLL_TIMEOUT_MS = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "1000"))

# Commit offsets after this many messages (batch processing)
BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "100"))

# Dead Letter Queue topic
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "cdc.dead_letter_queue")


# ============================================================
# MinIO (S3-Compatible Bronze Layer)
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Bucket for raw CDC events (Bronze layer in medallion architecture)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "cdc-bronze")

# Path pattern for organizing events: {bucket}/{table}/{date}/{hour}/{file}
# Example: cdc-bronze/customers/2024/01/15/14/events_001.json
MINIO_PATH_PATTERN = os.getenv(
    "MINIO_PATH_PATTERN",
    "{table}/{year}/{month:02d}/{day:02d}/{hour:02d}",
)

# Batch events into files of this size (reduces S3 PUT calls)
MINIO_BATCH_SIZE = int(os.getenv("MINIO_BATCH_SIZE", "100"))

# Max time to hold events before flushing (seconds)
MINIO_FLUSH_INTERVAL_S = int(os.getenv("MINIO_FLUSH_INTERVAL_S", "30"))


# ============================================================
# Target PostgreSQL (Replication Target)
# ============================================================

TARGET_PG_HOST = os.getenv("TARGET_PG_HOST", "127.0.0.1")
TARGET_PG_PORT = int(os.getenv("TARGET_PG_PORT", "5435"))
TARGET_PG_DB = os.getenv("TARGET_PG_DB", "target_db")
TARGET_PG_USER = os.getenv("TARGET_PG_USER", "target_user")
TARGET_PG_PASSWORD = os.getenv("TARGET_PG_PASSWORD", "target_password")


def get_target_pg_dsn() -> str:
    """Build connection string for target PostgreSQL."""
    return (
        f"host={TARGET_PG_HOST} "
        f"port={TARGET_PG_PORT} "
        f"dbname={TARGET_PG_DB} "
        f"user={TARGET_PG_USER} "
        f"password={TARGET_PG_PASSWORD}"
    )


# ============================================================
# Sink Configuration
# ============================================================

# Which sinks to enable (comma-separated): minio, postgres, both
ENABLED_SINKS = os.getenv("ENABLED_SINKS", "minio,postgres").split(",")


# ============================================================
# Processing Settings
# ============================================================

# Enable exactly-once semantics via event_id deduplication
ENABLE_DEDUPLICATION = os.getenv("ENABLE_DEDUPLICATION", "true").lower() == "true"

# How many recent event_ids to cache for deduplication
DEDUP_CACHE_SIZE = int(os.getenv("DEDUP_CACHE_SIZE", "100000"))

# Max retries before sending to DLQ
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Exponential backoff base (seconds)
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))
