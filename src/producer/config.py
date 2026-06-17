"""
CDC Producer - Configuration
==============================
All configuration for the CDC producer, loaded from environment variables.
Uses sensible defaults for local development.
"""

import os


# ============================================================
# Source PostgreSQL (Replication Connection)
# ============================================================

# Replication connections use the same host/port/db as regular connections
# but with the replication=database parameter
SOURCE_PG_HOST = os.getenv("SOURCE_PG_HOST", "127.0.0.1")
SOURCE_PG_PORT = int(os.getenv("SOURCE_PG_PORT", "5434"))
SOURCE_PG_DB = os.getenv("SOURCE_PG_DB", "source_db")
SOURCE_PG_USER = os.getenv("SOURCE_PG_USER", "cdc_user")
SOURCE_PG_PASSWORD = os.getenv("SOURCE_PG_PASSWORD", "cdc_password")

# The replication slot name created in Phase 3 init.sql
REPLICATION_SLOT = os.getenv("SOURCE_PG_REPLICATION_SLOT", "cdc_slot")

# The publication name created in Phase 3 init.sql
PUBLICATION_NAME = "cdc_publication"

# DSN for replication connection (replication=database is critical)
def get_replication_dsn() -> str:
    return (
        f"host={SOURCE_PG_HOST} "
        f"port={SOURCE_PG_PORT} "
        f"dbname={SOURCE_PG_DB} "
        f"user={SOURCE_PG_USER} "
        f"password={SOURCE_PG_PASSWORD} "
        f"replication=database"
    )

# DSN for regular queries (schema introspection, etc.)
def get_regular_dsn() -> str:
    return (
        f"host={SOURCE_PG_HOST} "
        f"port={SOURCE_PG_PORT} "
        f"dbname={SOURCE_PG_DB} "
        f"user={SOURCE_PG_USER} "
        f"password={SOURCE_PG_PASSWORD}"
    )


# ============================================================
# Kafka Producer
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Kafka topic naming: cdc.<source>.<schema>.<table>
TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "cdc.source.public")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "cdc.dead_letter_queue")

def get_topic_name(table_name: str) -> str:
    """Map a source table name to its Kafka topic name."""
    return f"{TOPIC_PREFIX}.{table_name}"


# ============================================================
# Producer Behaviour
# ============================================================

# How often to poll the WAL for new messages (milliseconds)
CDC_POLL_INTERVAL_MS = int(os.getenv("CDC_POLL_INTERVAL_MS", "200"))

# How often to send LSN feedback to PostgreSQL even if no new messages (seconds)
# Prevents connection timeout on idle streams
KEEPALIVE_INTERVAL_S = 10

# Max events to buffer per transaction before forcing a flush
# (safety valve against huge transactions filling memory)
MAX_BUFFER_SIZE = 10_000
