"""
Integration Test Utilities
===========================
Helper classes and functions for integration testing.

Provides:
- Kafka test helpers (producer, consumer, topic management)
- Database test helpers (connection, cleanup)
- MinIO test helpers
- Wait/retry utilities
"""

import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Generator, List, Optional

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

@dataclass
class IntegrationTestConfig:
    """Integration test configuration."""
    # Kafka
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_timeout_ms: int = 5000
    
    # Source PostgreSQL
    source_pg_host: str = "127.0.0.1"
    source_pg_port: int = 5434
    source_pg_db: str = "source_db"
    source_pg_user: str = "source_user"
    source_pg_password: str = "source_password"
    
    # Target PostgreSQL
    target_pg_host: str = "127.0.0.1"
    target_pg_port: int = 5435
    target_pg_db: str = "target_db"
    target_pg_user: str = "target_user"
    target_pg_password: str = "target_password"
    
    # MinIO
    minio_endpoint: str = "127.0.0.1:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "cdc-bronze"
    
    # Test settings
    default_timeout: float = 30.0
    poll_interval: float = 0.5

    @classmethod
    def from_env(cls) -> "IntegrationTestConfig":
        """Load config from environment variables."""
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            source_pg_host=os.getenv("SOURCE_PG_HOST", "127.0.0.1"),
            source_pg_port=int(os.getenv("SOURCE_PG_PORT", "5434")),
            source_pg_db=os.getenv("SOURCE_PG_DB", "source_db"),
            source_pg_user=os.getenv("SOURCE_PG_USER", "source_user"),
            source_pg_password=os.getenv("SOURCE_PG_PASSWORD", "source_password"),
            target_pg_host=os.getenv("TARGET_PG_HOST", "127.0.0.1"),
            target_pg_port=int(os.getenv("TARGET_PG_PORT", "5435")),
            target_pg_db=os.getenv("TARGET_PG_DB", "target_db"),
            target_pg_user=os.getenv("TARGET_PG_USER", "target_user"),
            target_pg_password=os.getenv("TARGET_PG_PASSWORD", "target_password"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )


# ============================================================
# Kafka Helpers
# ============================================================

class KafkaTestHelper:
    """
    Helper class for Kafka operations in tests.
    
    Provides:
    - Topic creation/deletion
    - Message production
    - Message consumption with timeout
    - DLQ inspection
    """
    
    def __init__(self, config: IntegrationTestConfig = None):
        from confluent_kafka import Producer, Consumer
        from confluent_kafka.admin import AdminClient, NewTopic
        
        self.config = config or IntegrationTestConfig.from_env()
        
        # Admin client for topic management
        self._admin = AdminClient({
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
        })
        
        # Producer for test messages
        self._producer = Producer({
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "acks": "all",
        })
        
        # Track created topics for cleanup
        self._created_topics: List[str] = []
    
    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
    ) -> bool:
        """Create a Kafka topic."""
        from confluent_kafka.admin import NewTopic
        
        new_topic = NewTopic(
            name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        
        futures = self._admin.create_topics([new_topic])
        
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Created topic: {topic}")
                self._created_topics.append(topic)
                return True
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.debug(f"Topic {topic} already exists")
                    return True
                logger.error(f"Failed to create topic {topic}: {e}")
                return False
        
        return False
    
    def delete_topic(self, name: str) -> bool:
        """Delete a Kafka topic."""
        futures = self._admin.delete_topics([name])
        
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Deleted topic: {topic}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete topic {topic}: {e}")
                return False
        
        return False
    
    def produce(
        self,
        topic: str,
        value: dict,
        key: str = None,
    ) -> None:
        """Produce a message to a topic."""
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(value).encode("utf-8"),
        )
        self._producer.flush()
    
    def produce_cdc_event(
        self,
        topic: str,
        operation: str = "INSERT",
        table: str = "customers",
        data: dict = None,
        event_id: str = None,
    ) -> str:
        """
        Produce a valid CDC event to a topic.
        
        Returns the event_id.
        """
        event_id = event_id or str(uuid.uuid4())
        
        event = {
            "event_id": event_id,
            "operation": operation,
            "source": {
                "database": "source_db",
                "schema": "public",
                "table": table,
            },
            "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "before": None if operation == "INSERT" else (data or {"id": 1}),
            "after": data or {"id": 1, "name": "Test"},
        }
        
        if operation == "DELETE":
            event["after"] = None
            event["before"] = data or {"id": 1}
        
        self.produce(topic, event, key=f"{table}:{data.get('id', 1) if data else 1}")
        
        return event_id
    
    def consume_messages(
        self,
        topic: str,
        count: int = 1,
        timeout: float = None,
        group_id: str = None,
    ) -> List[dict]:
        """
        Consume messages from a topic.
        
        Args:
            topic: Topic to consume from
            count: Number of messages to consume
            timeout: Max time to wait
            group_id: Consumer group (auto-generated if not provided)
        
        Returns:
            List of message values (as dicts)
        """
        from confluent_kafka import Consumer
        
        timeout = timeout or self.config.default_timeout
        group_id = group_id or f"test-group-{uuid.uuid4().hex[:8]}"
        
        consumer = Consumer({
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        })
        
        consumer.subscribe([topic])
        
        messages = []
        start_time = time.time()
        
        try:
            while len(messages) < count:
                if time.time() - start_time > timeout:
                    break
                
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    messages.append(value)
                except json.JSONDecodeError:
                    messages.append({"raw": msg.value().decode("utf-8")})
        finally:
            consumer.close()
        
        return messages
    
    def consume_dlq_messages(
        self,
        dlq_topic: str = "cdc.dead_letter_queue",
        count: int = 1,
        timeout: float = None,
    ) -> List[dict]:
        """Consume messages from the Dead Letter Queue."""
        return self.consume_messages(
            topic=dlq_topic,
            count=count,
            timeout=timeout,
        )
    
    def cleanup(self) -> None:
        """Clean up created topics."""
        for topic in self._created_topics:
            self.delete_topic(topic)
        self._created_topics.clear()


# ============================================================
# Database Helpers
# ============================================================

class PostgresTestHelper:
    """
    Helper class for PostgreSQL operations in tests.
    
    Provides:
    - Connection management
    - Test data insertion
    - Cleanup utilities
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ):
        import psycopg2
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self._conn = None
        self._test_records: List[tuple] = []  # (table, id) pairs for cleanup
    
    @property
    def connection(self):
        """Get or create a database connection."""
        import psycopg2
        
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        return self._conn
    
    def execute(self, query: str, params: tuple = None) -> Any:
        """Execute a query and return results."""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        
        try:
            result = cursor.fetchall()
        except:
            result = None
        
        return result
    
    def execute_commit(self, query: str, params: tuple = None) -> None:
        """Execute a query and commit."""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
    
    def insert_customer(
        self,
        first_name: str = "Test",
        last_name: str = "User",
        email: str = None,
    ) -> int:
        """Insert a test customer and return the ID."""
        email = email or f"test_{uuid.uuid4().hex[:8]}@test.com"
        
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO customers (first_name, last_name, email, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING id
            """,
            (first_name, last_name, email),
        )
        customer_id = cursor.fetchone()[0]
        self.connection.commit()
        
        self._test_records.append(("customers", customer_id))
        
        return customer_id
    
    def insert_order(
        self,
        customer_id: int,
        total: float = 100.0,
    ) -> int:
        """Insert a test order and return the ID."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO orders (customer_id, total_amount, status, created_at)
            VALUES (%s, %s, 'pending', NOW())
            RETURNING id
            """,
            (customer_id, total),
        )
        order_id = cursor.fetchone()[0]
        self.connection.commit()
        
        self._test_records.append(("orders", order_id))
        
        return order_id
    
    def get_customer(self, customer_id: int) -> Optional[dict]:
        """Get a customer by ID."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT id, first_name, last_name, email FROM customers WHERE id = %s",
            (customer_id,),
        )
        row = cursor.fetchone()
        
        if row:
            return {
                "id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "email": row[3],
            }
        return None
    
    def cleanup(self) -> None:
        """Clean up test records."""
        cursor = self.connection.cursor()
        
        # Delete in reverse order (handle foreign keys)
        for table, record_id in reversed(self._test_records):
            try:
                cursor.execute(f"DELETE FROM {table} WHERE id = %s", (record_id,))
            except Exception as e:
                logger.warning(f"Failed to delete {table}:{record_id}: {e}")
        
        self.connection.commit()
        self._test_records.clear()
    
    def close(self) -> None:
        """Close the connection."""
        if self._conn:
            self._conn.close()


# ============================================================
# MinIO Helpers
# ============================================================

class MinIOTestHelper:
    """
    Helper class for MinIO operations in tests.
    
    Provides:
    - Bucket management
    - Object listing/reading
    - Cleanup utilities
    """
    
    def __init__(self, config: IntegrationTestConfig = None):
        from minio import Minio
        
        self.config = config or IntegrationTestConfig.from_env()
        
        self._client = Minio(
            self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=False,
        )
        
        self._test_objects: List[tuple] = []  # (bucket, key) pairs
    
    @property
    def client(self):
        """Get the MinIO client."""
        return self._client
    
    def ensure_bucket(self, bucket: str = None) -> bool:
        """Ensure a bucket exists."""
        bucket = bucket or self.config.minio_bucket
        
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            logger.info(f"Created bucket: {bucket}")
            return True
        return False
    
    def list_objects(
        self,
        bucket: str = None,
        prefix: str = "",
    ) -> List[str]:
        """List objects in a bucket."""
        bucket = bucket or self.config.minio_bucket
        
        objects = self._client.list_objects(bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    
    def get_object(self, key: str, bucket: str = None) -> bytes:
        """Get an object's content."""
        bucket = bucket or self.config.minio_bucket
        
        response = self._client.get_object(bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
    
    def get_object_json(self, key: str, bucket: str = None) -> dict:
        """Get an object's content as JSON."""
        content = self.get_object(key, bucket)
        return json.loads(content.decode("utf-8"))
    
    def count_objects(
        self,
        bucket: str = None,
        prefix: str = "",
    ) -> int:
        """Count objects in a bucket."""
        return len(self.list_objects(bucket, prefix))
    
    def cleanup(self, prefix: str = None) -> None:
        """Clean up test objects."""
        bucket = self.config.minio_bucket
        
        if prefix:
            objects = self.list_objects(bucket, prefix)
            for obj in objects:
                try:
                    self._client.remove_object(bucket, obj)
                except Exception as e:
                    logger.warning(f"Failed to delete {obj}: {e}")


# ============================================================
# Wait/Retry Utilities
# ============================================================

def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = 30.0,
    poll_interval: float = 0.5,
    description: str = "condition",
) -> bool:
    """
    Wait for a condition to become true.
    
    Args:
        condition: Callable that returns True when condition is met
        timeout: Max time to wait in seconds
        poll_interval: Time between checks
        description: Description for logging
    
    Returns:
        True if condition was met, False if timeout
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            if condition():
                return True
        except Exception as e:
            logger.debug(f"Condition check failed: {e}")
        
        time.sleep(poll_interval)
    
    logger.warning(f"Timeout waiting for: {description}")
    return False


def retry_on_failure(
    func: Callable,
    max_retries: int = 3,
    delay: float = 1.0,
    exceptions: tuple = (Exception,),
) -> Any:
    """
    Retry a function on failure.
    
    Args:
        func: Function to call
        max_retries: Maximum number of retries
        delay: Delay between retries
        exceptions: Exceptions to catch and retry
    
    Returns:
        Function result if successful
    
    Raises:
        Last exception if all retries fail
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            last_error = e
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    
    raise last_error


# ============================================================
# Test Event Generators
# ============================================================

def create_test_cdc_event(
    operation: str = "INSERT",
    table: str = "customers",
    data: dict = None,
    event_id: str = None,
) -> dict:
    """Create a test CDC event dictionary."""
    event_id = event_id or str(uuid.uuid4())
    data = data or {"id": 1, "name": "Test"}
    
    event = {
        "event_id": event_id,
        "operation": operation,
        "source": {
            "database": "source_db",
            "schema": "public",
            "table": table,
        },
        "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "before": None,
        "after": data,
    }
    
    if operation == "UPDATE":
        event["before"] = data
    elif operation == "DELETE":
        event["before"] = data
        event["after"] = None
    
    return event


def create_invalid_cdc_event(
    missing_field: str = "operation",
    invalid_value: str = None,
) -> dict:
    """Create an invalid CDC event for testing error handling."""
    event = create_test_cdc_event()
    
    if missing_field:
        event.pop(missing_field, None)
    
    if invalid_value:
        event["operation"] = invalid_value
    
    return event
