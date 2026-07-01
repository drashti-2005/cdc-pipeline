"""
CDC Producer - Kafka Publisher
================================
Reads CDC events from PostgreSQL WAL (via wal_reader.py) and publishes
them to Kafka topics.

Run this as the main CDC producer process:
    python -m src.producer.kafka_producer

How it works:
    1. Connect to PostgreSQL replication slot
    2. Start streaming WAL changes
    3. For each committed transaction:
       a. Determine the target Kafka topic for each event
       b. Produce all events to Kafka
       c. Wait for Kafka delivery confirmations
       d. Only then confirm the LSN to PostgreSQL
    4. On error: log, back off, retry

The key guarantee: we NEVER advance the PostgreSQL LSN until Kafka
confirms delivery. This means on crash/restart, we may re-produce some
events (causing duplicates), but we will NEVER lose events.
Deduplication in the consumer (Phase 10) handles the duplicates.
"""

import json
import logging
import signal
import sys
import time

from confluent_kafka import Producer, KafkaError

from producer.config import (
    DLQ_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    get_topic_name,
)
from producer.wal_reader import WALReader
from schemas.cdc_event import CDCEvent, DLQEvent

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdc.producer")


# ============================================================
# Kafka Producer Setup
# ============================================================

def create_kafka_producer() -> Producer:
    """Create and return a configured Kafka producer."""
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Retry up to 3 times on transient errors
        "retries": 3,
        "retry.backoff.ms": 500,
        # Enable idempotent producer (no duplicate messages from producer side)
        "enable.idempotence": True,
        # Wait for all in-sync replicas to acknowledge (strongest durability)
        "acks": "all",
        # How long to wait for messages to be sent (5 seconds)
        "delivery.timeout.ms": 5000,
        # Batch settings for throughput
        "batch.size": 16384,       # 16KB batch
        "linger.ms": 5,            # Wait up to 5ms to accumulate a batch
    })


# ============================================================
# Event Publishing
# ============================================================

def get_topic_for_event(event: CDCEvent) -> str:
    """Determine the Kafka topic for a given CDC event."""
    return get_topic_name(event.source.table)


def publish_transaction(
    producer: Producer,
    events: list[CDCEvent],
    transaction_xid: int | None = None,
) -> tuple[int, int]:
    """
    Publish all events from a single PostgreSQL transaction to Kafka.
    Returns (success_count, error_count).

    All events are produced synchronously (flush waits for ack).
    We do NOT advance the PostgreSQL LSN until this function returns
    successfully.
    """
    success_count = 0
    error_count = 0
    delivery_errors: list[str] = []

    def delivery_callback(err, msg):
        nonlocal success_count, error_count
        if err:
            error_msg = f"Delivery failed for {msg.topic()}: {err}"
            delivery_errors.append(error_msg)
            logger.error(error_msg)
            error_count += 1
        else:
            success_count += 1
            logger.debug(
                f"Produced to {msg.topic()} "
                f"[p={msg.partition()}, o={msg.offset()}] "
                f"op={msg.key().decode() if msg.key() else None}"
            )

    for event in events:
        topic = get_topic_for_event(event)

        # Partition key = entity primary key (ensures per-entity ordering)
        partition_key = event.get_partition_key().encode("utf-8")

        # Serialize event to JSON bytes
        event_bytes = event.to_json_bytes()

        # Produce to Kafka (non-blocking — callback called on flush)
        producer.produce(
            topic=topic,
            key=partition_key,
            value=event_bytes,
            callback=delivery_callback,
        )

    # Flush all buffered messages and wait for delivery confirmations
    # This is where we block until Kafka acknowledges every message
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        logger.error(f"{remaining} messages were not delivered within timeout")
        error_count += remaining

    if delivery_errors:
        logger.error(
            f"Transaction xid={transaction_xid} had {error_count} delivery errors"
        )

    return success_count, error_count


def publish_to_dlq(producer: Producer, event: CDCEvent, error_message: str, error_type: str):
    """Send a failed event to the Dead Letter Queue topic."""
    dlq_event = DLQEvent(
        original_event=event,
        error_message=error_message,
        error_type=error_type,
    )
    producer.produce(
        topic=DLQ_TOPIC,
        key=event.get_partition_key().encode("utf-8"),
        value=dlq_event.to_json_bytes(),
    )
    producer.flush(timeout=10)
    logger.warning(f"Event sent to DLQ: table={event.source.table} op={event.operation}")


# ============================================================
# Main Producer Loop
# ============================================================

class CDCProducer:
    """
    The main CDC producer — connects WAL reader to Kafka publisher.
    Handles graceful shutdown and retry logic.
    """

    def __init__(self):
        self.wal_reader = WALReader()
        self.kafka_producer = None
        self.running = False
        self.stats = {
            "transactions_processed": 0,
            "events_published": 0,
            "errors": 0,
            "start_time": None,
        }

    def setup_signal_handlers(self):
        """Handle Ctrl+C and SIGTERM for graceful shutdown."""
        def shutdown_handler(sig, frame):
            logger.info(f"Received signal {sig} — shutting down gracefully...")
            self.running = False

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

    def print_stats(self):
        """Print pipeline throughput statistics."""
        elapsed = time.time() - self.stats["start_time"]
        events_per_sec = self.stats["events_published"] / max(elapsed, 1)
        logger.info(
            f"Stats | "
            f"txns={self.stats['transactions_processed']} | "
            f"events={self.stats['events_published']} | "
            f"errors={self.stats['errors']} | "
            f"throughput={events_per_sec:.1f} events/s"
        )

    def run(self):
        """Main loop — read from WAL, publish to Kafka."""
        self.setup_signal_handlers()
        self.running = True
        self.stats["start_time"] = time.time()

        retry_delay = 1  # Start with 1s, back off to 30s max

        logger.info("=" * 60)
        logger.info("CDC Producer Starting")
        logger.info("=" * 60)

        while self.running:
            try:
                # Connect to PostgreSQL replication
                self.wal_reader.connect()
                self.wal_reader.start_replication()

                # Create Kafka producer
                self.kafka_producer = create_kafka_producer()

                retry_delay = 1  # Reset backoff on successful connection
                logger.info("CDC Producer running — streaming WAL changes to Kafka")
                logger.info("Press Ctrl+C to stop gracefully")

                last_stats_time = time.time()

                # Main processing loop
                for events, commit_lsn_int in self.wal_reader.read_transactions():
                    if not self.running:
                        break

                    xid = events[0].source.transaction_id if events else None

                    logger.info(
                        f"Transaction xid={xid} | "
                        f"events={len(events)} | "
                        f"tables={list({e.source.table for e in events})}"
                    )

                    # Publish all events to Kafka
                    success, errors = publish_transaction(
                        self.kafka_producer,
                        events,
                        transaction_xid=xid,
                    )

                    if errors == 0:
                        # All events delivered — safe to advance LSN
                        self.wal_reader.confirm_lsn(commit_lsn_int)
                        self.stats["transactions_processed"] += 1
                        self.stats["events_published"] += success
                    else:
                        self.stats["errors"] += errors
                        logger.error(
                            f"Transaction xid={xid} had errors — "
                            "LSN NOT advanced (will retry on restart)"
                        )

                    # Print stats every 30 seconds
                    if time.time() - last_stats_time >= 30:
                        self.print_stats()
                        last_stats_time = time.time()

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                self.running = False

            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"Producer error: {e}", exc_info=True)

                if self.running:
                    logger.info(f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)  # Exponential backoff, max 30s

            finally:
                self.wal_reader.close()
                if self.kafka_producer:
                    self.kafka_producer.flush(timeout=10)

        self.print_stats()
        logger.info("CDC Producer stopped")


# ============================================================
# Entry Point
# ============================================================

def main():
    producer = CDCProducer()
    producer.run()


if __name__ == "__main__":
    main()
