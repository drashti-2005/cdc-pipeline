"""
CDC Pipeline - Kafka Topic Creator
====================================
Creates all required Kafka topics for the CDC pipeline with
proper partition counts, replication factors, and configurations.

Usage:
    python scripts/create_kafka_topics.py            # Create all topics
    python scripts/create_kafka_topics.py --test     # Create topics + produce/consume test message
    python scripts/create_kafka_topics.py --delete   # Delete all CDC topics (use with caution)

Why we create topics explicitly:
    - Control over partition count (can't decrease later!)
    - Explicit retention and cleanup policies
    - Governance: no accidental topic creation by misconfigured producers
    - Documentation: topic config is code, versioned in git
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

# Topic definitions: name → (partitions, config)
# Higher-volume tables get more partitions for parallel processing
TOPICS = {
    # CDC event topics (one per source table)
    "cdc.source.public.customers": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "cleanup.policy": "delete",                      # Delete old messages
            "min.insync.replicas": "1",
        },
    },
    "cdc.source.public.products": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            "cleanup.policy": "delete",
            "min.insync.replicas": "1",
        },
    },
    "cdc.source.public.orders": {
        "partitions": 6,  # Higher volume → more partitions
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            "cleanup.policy": "delete",
            "min.insync.replicas": "1",
        },
    },
    "cdc.source.public.order_items": {
        "partitions": 6,  # Match orders (same volume pattern)
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            "cleanup.policy": "delete",
            "min.insync.replicas": "1",
        },
    },
    # Dead Letter Queue - for messages that fail processing
    "cdc.dead_letter_queue": {
        "partitions": 1,  # Low volume, ordering not critical
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days (keep longer for investigation)
            "cleanup.policy": "delete",
        },
    },
}


# ============================================================
# Topic Management Functions
# ============================================================

def create_topics(admin_client):
    """Create all CDC topics if they don't already exist."""
    # Get existing topics
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    topics_to_create = []
    for topic_name, topic_config in TOPICS.items():
        if topic_name in existing_topics:
            print(f"  ⏭  Topic '{topic_name}' already exists, skipping")
            continue

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=topic_config["partitions"],
            replication_factor=topic_config["replication_factor"],
            config=topic_config.get("config", {}),
        )
        topics_to_create.append(new_topic)

    if not topics_to_create:
        print("\n  All topics already exist. Nothing to create.")
        return

    # Create topics (returns a dict of futures)
    futures = admin_client.create_topics(topics_to_create)

    # Wait for each topic creation to complete
    for topic_name, future in futures.items():
        try:
            future.result()  # Blocks until topic is created or error
            config = TOPICS[topic_name]
            print(f"  ✓  Created topic '{topic_name}' (partitions={config['partitions']})")
        except Exception as e:
            print(f"  ✗  Failed to create topic '{topic_name}': {e}")


def delete_topics(admin_client):
    """Delete all CDC topics. USE WITH CAUTION."""
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    topics_to_delete = [t for t in TOPICS.keys() if t in existing_topics]

    if not topics_to_delete:
        print("  No CDC topics found to delete.")
        return

    futures = admin_client.delete_topics(topics_to_delete)
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"  ✓  Deleted topic '{topic_name}'")
        except Exception as e:
            print(f"  ✗  Failed to delete topic '{topic_name}': {e}")


def list_topics(admin_client):
    """List all topics with their partition counts."""
    metadata = admin_client.list_topics(timeout=10)

    print("\n  All Kafka Topics:")
    print("  " + "-" * 55)
    print(f"  {'Topic':<40} {'Partitions':<10}")
    print("  " + "-" * 55)

    for topic_name in sorted(metadata.topics.keys()):
        if topic_name.startswith("_"):  # Skip internal topics
            continue
        partitions = len(metadata.topics[topic_name].partitions)
        marker = " ← CDC" if topic_name.startswith("cdc.") else ""
        print(f"  {topic_name:<40} {partitions:<10}{marker}")


# ============================================================
# Test Function - Produce and Consume a test CDC event
# ============================================================

def test_produce_consume():
    """Produce a test CDC event and consume it back to validate the pipeline."""
    topic = "cdc.source.public.customers"

    # Create a sample CDC event matching our schema
    test_event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": {
            "database": "source_db",
            "schema": "public",
            "table": "customers",
            "transaction_id": 99999,
            "lsn": "0/TEST123",
        },
        "operation": "INSERT",
        "before": None,
        "after": {
            "id": 999,
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "User",
            "phone": "+1-555-0000",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    # --- PRODUCE ---
    print("\n  [Producer] Sending test CDC event...")
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # Partition key = entity primary key (ensures ordering per entity)
    partition_key = str(test_event["after"]["id"]).encode("utf-8")
    message_value = json.dumps(test_event).encode("utf-8")

    delivery_confirmed = [False]

    def delivery_callback(err, msg):
        if err:
            print(f"  [Producer] ✗ Delivery failed: {err}")
        else:
            print(f"  [Producer] ✓ Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")
            delivery_confirmed[0] = True

    producer.produce(
        topic=topic,
        key=partition_key,
        value=message_value,
        callback=delivery_callback,
    )
    producer.flush(timeout=10)  # Wait for delivery confirmation

    if not delivery_confirmed[0]:
        print("  [Producer] ✗ Message was not confirmed delivered")
        return False

    # --- CONSUME ---
    print("  [Consumer] Reading back the test message...")
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "cdc-test-group-" + str(uuid.uuid4())[:8],  # Unique group to read from beginning
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    received = False
    start_time = time.time()
    timeout_seconds = 15

    while time.time() - start_time < timeout_seconds:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"  [Consumer] ✗ Error: {msg.error()}")
            break

        # Deserialize and validate
        received_event = json.loads(msg.value().decode("utf-8"))
        if received_event["event_id"] == test_event["event_id"]:
            print(f"  [Consumer] ✓ Received test event (offset={msg.offset()})")
            print(f"  [Consumer]   Operation: {received_event['operation']}")
            print(f"  [Consumer]   Table: {received_event['source']['table']}")
            print(f"  [Consumer]   Entity ID: {received_event['after']['id']}")
            received = True
            break

    consumer.close()

    if not received:
        print(f"  [Consumer] ✗ Did not receive test message within {timeout_seconds}s")
        return False

    print("\n  ✓ End-to-end test PASSED: Produce → Kafka → Consume working correctly")
    return True


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline - Kafka Topic Manager")
    parser.add_argument("--test", action="store_true", help="Run produce/consume test after creating topics")
    parser.add_argument("--delete", action="store_true", help="Delete all CDC topics (dangerous!)")
    parser.add_argument("--list", action="store_true", help="List all topics")
    args = parser.parse_args()

    print("=" * 60)
    print("CDC Pipeline - Kafka Topic Manager")
    print("=" * 60)
    print(f"  Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print()

    # Create admin client
    try:
        admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        # Test connectivity
        admin_client.list_topics(timeout=5)
        print("  ✓ Connected to Kafka broker")
    except Exception as e:
        print(f"  ✗ Cannot connect to Kafka: {e}")
        print("  Make sure the kafka container is running:")
        print("  docker compose -f docker/docker-compose.yml --env-file .env up -d kafka")
        sys.exit(1)

    if args.delete:
        print("\n  ⚠️  Deleting all CDC topics...")
        delete_topics(admin_client)
    elif args.list:
        list_topics(admin_client)
    else:
        print("\n  Creating CDC topics...")
        create_topics(admin_client)
        print()
        list_topics(admin_client)

    if args.test:
        print("\n" + "-" * 60)
        print("  Running end-to-end produce/consume test...")
        print("-" * 60)
        success = test_produce_consume()
        if not success:
            sys.exit(1)

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
