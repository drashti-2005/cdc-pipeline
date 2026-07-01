"""
CDC Kafka Consumer - Main Entry Point
======================================
Reads CDC events from Kafka and routes them to sinks.

This is the CONSUMER side of our CDC pipeline:
  Producer (Phase 5): PostgreSQL WAL → Kafka
  Consumer (Phase 6): Kafka → MinIO + Target PostgreSQL

HOW KAFKA CONSUMERS WORK
------------------------
1. Consumer subscribes to topics
2. Kafka assigns partitions to consumer (consumer group)
3. Consumer polls for messages
4. Consumer processes messages
5. Consumer commits offsets (marks messages as processed)

CONSUMER GROUPS
---------------
Multiple consumers with same group_id share the work:
- Each partition is assigned to ONE consumer
- If consumer dies, partitions are rebalanced
- Scales horizontally by adding more consumers

AT-LEAST-ONCE DELIVERY
----------------------
Our pattern:
1. Poll messages
2. Process all messages
3. Commit offsets

If we crash between 2 and 3, messages are redelivered.
That's why our sinks are idempotent!

FOR INTERVIEW
-------------
Q: What's the difference between auto.commit and manual commit?
A: Auto-commit commits periodically (risky: may lose messages).
   Manual commit gives us control: commit AFTER successful processing.

Q: How do you handle slow consumers?
A: Increase partitions, add more consumers, or use batch processing.

Q: What happens during rebalancing?
A: Consumers pause, partitions are reassigned, then resume.
   We lose in-flight messages (will be redelivered).
"""

import logging
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from . import config
from consumer.event_router import EventRouter
from schemas.cdc_event import CDCEvent
from metrics import (
    start_metrics_server,
    record_consumer_event,
    CONSUMER_PROCESSING_DURATION,
    CONSUMER_LAG,
    CONSUMER_OFFSET,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class CDCConsumer:
    """
    Main Kafka consumer that reads CDC events and routes to sinks.

    Features:
    - Manual offset commit (at-least-once delivery)
    - Graceful shutdown on SIGINT/SIGTERM
    - Batch processing for efficiency
    - Error handling with DLQ

    SIMPLE EXPLANATION:
    Like a factory assembly line:
    1. Raw materials arrive (CDC events from Kafka)
    2. Workers process them (route to sinks)
    3. Mark as done (commit offsets)
    4. If something breaks, set aside for repair (DLQ)
    """

    def __init__(self):
        """Initialize Kafka consumer and event router."""
        self._consumer = Consumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": config.CONSUMER_GROUP_ID,
            "auto.offset.reset": config.AUTO_OFFSET_RESET,
            "enable.auto.commit": False,  # We commit manually!
        })

        self._router = EventRouter()
        self._running = False
        self._messages_processed = 0

        logger.info(
            f"CDC Consumer initialized, group={config.CONSUMER_GROUP_ID}, "
            f"topics={config.KAFKA_TOPICS}"
        )

    def setup_signal_handlers(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM."""
        def shutdown_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self._running = False

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

    def run(self) -> None:
        """
        Main consumer loop.

        1. Subscribe to topics
        2. Poll for messages
        3. Route each message to sinks
        4. Commit offsets after batch
        5. Repeat until shutdown signal
        """
        self.setup_signal_handlers()
        self._running = True

        # Start Prometheus metrics server
        try:
            start_metrics_server(port=8000)
        except Exception as e:
            logger.warning(f"Could not start metrics server: {e}")

        # Subscribe to configured topics
        self._consumer.subscribe(config.KAFKA_TOPICS)
        logger.info(f"Subscribed to topics: {config.KAFKA_TOPICS}")

        try:
            while self._running:
                self._poll_and_process()

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.exception(f"Fatal error in consumer loop: {e}")
            raise
        finally:
            self._shutdown()

    def _poll_and_process(self) -> None:
        """Poll Kafka for messages and process them."""
        msg = self._consumer.poll(timeout=config.CONSUMER_POLL_TIMEOUT_MS / 1000)

        if msg is None:
            # No message available
            return

        if msg.error():
            self._handle_error(msg.error())
            return

        # Record metrics
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        record_consumer_event(topic)
        CONSUMER_OFFSET.labels(topic=topic, partition=partition).set(offset)

        # Process the message
        try:
            start_time = time.time()
            event = self._parse_message(msg)
            if event:
                success = self._router.route(event)
                processing_time = time.time() - start_time
                CONSUMER_PROCESSING_DURATION.observe(processing_time)
                
                if success:
                    self._messages_processed += 1

                    # Commit offset after successful processing
                    # This is at-least-once: if we crash before commit,
                    # the message will be redelivered
                    self._consumer.commit(asynchronous=False)

                    if self._messages_processed % 100 == 0:
                        logger.info(f"Processed {self._messages_processed} messages")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Still commit to avoid infinite retry of bad messages
            # The event router handles sending to DLQ
            self._consumer.commit(asynchronous=False)

    def _parse_message(self, msg) -> Optional[CDCEvent]:
        """Parse a Kafka message into a CDCEvent."""
        try:
            value = msg.value()
            if value is None:
                logger.warning("Received message with null value")
                return None

            event = CDCEvent.from_json_bytes(value)
            logger.debug(
                f"Received event: {event.operation.value} on {event.source.table} "
                f"(partition={msg.partition()}, offset={msg.offset()})"
            )
            return event

        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            logger.debug(f"Raw message value: {msg.value()}")
            return None

    def _handle_error(self, error: KafkaError) -> None:
        """Handle Kafka errors."""
        if error.code() == KafkaError._PARTITION_EOF:
            # Normal: reached end of partition
            logger.debug(f"End of partition reached: {error}")
        elif error.code() == KafkaError._ALL_BROKERS_DOWN:
            logger.error("All brokers down, will retry...")
        else:
            logger.error(f"Kafka error: {error}")
            raise KafkaException(error)

    def _shutdown(self) -> None:
        """Graceful shutdown: flush sinks and close connections."""
        logger.info("Shutting down CDC consumer...")

        # Flush and close event router (sinks)
        self._router.close()

        # Close Kafka consumer
        self._consumer.close()

        logger.info(
            f"CDC consumer shutdown complete. "
            f"Total messages processed: {self._messages_processed}"
        )

    # ========================================================
    # Metrics (for monitoring)
    # ========================================================

    def get_stats(self) -> dict:
        """Get consumer statistics."""
        return {
            "messages_processed": self._messages_processed,
            "router_stats": self._router.get_stats(),
        }


def main():
    """Entry point for the CDC consumer."""
    logger.info("=" * 60)
    logger.info("CDC Consumer Starting")
    logger.info("=" * 60)
    logger.info(f"Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topics: {config.KAFKA_TOPICS}")
    logger.info(f"MinIO: {config.MINIO_ENDPOINT}")
    logger.info(f"Target PG: {config.TARGET_PG_HOST}:{config.TARGET_PG_PORT}")
    logger.info("=" * 60)

    consumer = CDCConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
