"""
Pytest Fixtures for CDC Pipeline Tests
=======================================
Shared fixtures for unit, integration, and E2E tests.

WHAT ARE FIXTURES?
------------------
Fixtures are reusable setup/teardown functions that provide
test data or resources. They're like "before/after" hooks.

Example:
    def test_something(sample_cdc_event):
        # sample_cdc_event is automatically created by the fixture
        assert sample_cdc_event.operation == "INSERT"

WHY USE FIXTURES?
-----------------
1. DRY (Don't Repeat Yourself) - Share setup code
2. Isolation - Each test gets fresh resources
3. Cleanup - Automatic teardown after tests
4. Composition - Fixtures can use other fixtures
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional
from unittest.mock import MagicMock, patch

import pytest

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.schemas.cdc_event import CDCEvent, OperationType, SourceInfo


# ============================================================
# Environment Fixtures
# ============================================================

@pytest.fixture(scope="session")
def docker_services_available():
    """
    Check if Docker services are available.
    Skip integration tests if not.
    """
    import socket
    
    services = {
        "kafka": ("127.0.0.1", 9092),
        "postgres-source": ("127.0.0.1", 5434),
        "postgres-target": ("127.0.0.1", 5435),
        "minio": ("127.0.0.1", 9000),
    }
    
    available = {}
    for name, (host, port) in services.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            result = sock.connect_ex((host, port))
            available[name] = (result == 0)
        except:
            available[name] = False
        finally:
            sock.close()
    
    return available


@pytest.fixture
def require_docker(docker_services_available):
    """Skip test if Docker services aren't running."""
    if not all(docker_services_available.values()):
        missing = [k for k, v in docker_services_available.items() if not v]
        pytest.skip(f"Docker services not available: {missing}")


# ============================================================
# CDC Event Fixtures
# ============================================================

@pytest.fixture
def sample_source_info() -> SourceInfo:
    """Create a sample SourceInfo object."""
    return SourceInfo(
        database="source_db",
        schema_name="public",
        table="customers",
        transaction_id=12345,
        lsn="0/ABC123",
    )


@pytest.fixture
def sample_customer_data() -> Dict:
    """Sample customer data for INSERT/UPDATE events."""
    return {
        "id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "created_at": "2024-01-15T10:30:00Z",
    }


@pytest.fixture
def sample_insert_event(sample_source_info, sample_customer_data) -> CDCEvent:
    """Create a sample INSERT CDC event."""
    return CDCEvent(
        event_id=str(uuid.uuid4()),
        operation=OperationType.INSERT,
        source=sample_source_info,
        before=None,
        after=sample_customer_data,
    )


@pytest.fixture
def sample_update_event(sample_source_info, sample_customer_data) -> CDCEvent:
    """Create a sample UPDATE CDC event."""
    before_data = sample_customer_data.copy()
    after_data = sample_customer_data.copy()
    after_data["email"] = "john.updated@example.com"
    
    return CDCEvent(
        event_id=str(uuid.uuid4()),
        operation=OperationType.UPDATE,
        source=sample_source_info,
        before=before_data,
        after=after_data,
    )


@pytest.fixture
def sample_delete_event(sample_source_info, sample_customer_data) -> CDCEvent:
    """Create a sample DELETE CDC event."""
    return CDCEvent(
        event_id=str(uuid.uuid4()),
        operation=OperationType.DELETE,
        source=sample_source_info,
        before=sample_customer_data,
        after=None,
    )


@pytest.fixture
def sample_events_batch(sample_source_info) -> List[CDCEvent]:
    """Create a batch of mixed CDC events for testing."""
    events = []
    
    # 3 INSERT events
    for i in range(3):
        events.append(CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.INSERT,
            source=sample_source_info,
            before=None,
            after={"id": i + 1, "first_name": f"User{i}", "last_name": "Test"},
        ))
    
    # 2 UPDATE events
    for i in range(2):
        events.append(CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=OperationType.UPDATE,
            source=sample_source_info,
            before={"id": i + 1, "first_name": f"User{i}"},
            after={"id": i + 1, "first_name": f"UpdatedUser{i}"},
        ))
    
    # 1 DELETE event
    events.append(CDCEvent(
        event_id=str(uuid.uuid4()),
        operation=OperationType.DELETE,
        source=sample_source_info,
        before={"id": 3, "first_name": "User2"},
        after=None,
    ))
    
    return events


# ============================================================
# Mock Fixtures
# ============================================================

@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for unit tests."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.flush = MagicMock()
    producer.poll = MagicMock(return_value=0)
    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer for unit tests."""
    consumer = MagicMock()
    consumer.subscribe = MagicMock()
    consumer.poll = MagicMock(return_value=None)
    consumer.commit = MagicMock()
    consumer.close = MagicMock()
    return consumer


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client for unit tests."""
    client = MagicMock()
    client.bucket_exists = MagicMock(return_value=True)
    client.put_object = MagicMock()
    client.get_object = MagicMock()
    return client


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for unit tests."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.execute = MagicMock()
    cursor.fetchone = MagicMock(return_value=None)
    cursor.fetchall = MagicMock(return_value=[])
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()
    return conn


# ============================================================
# Database Fixtures (Integration Tests)
# ============================================================

@pytest.fixture
def source_db_connection(require_docker):
    """
    Connection to source PostgreSQL for integration tests.
    Automatically rolls back changes after test.
    """
    import psycopg2
    
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5434,
        database="source_db",
        user="cdc_user",
        password="cdc_password",
    )
    conn.autocommit = False
    
    yield conn
    
    # Rollback any changes made during test
    conn.rollback()
    conn.close()


@pytest.fixture
def target_db_connection(require_docker):
    """
    Connection to target PostgreSQL for integration tests.
    Automatically rolls back changes after test.
    """
    import psycopg2
    
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5435,
        database="target_db",
        user="target_user",
        password="target_password",
    )
    conn.autocommit = False
    
    yield conn
    
    conn.rollback()
    conn.close()


# ============================================================
# Kafka Fixtures (Integration Tests)
# ============================================================

@pytest.fixture
def kafka_admin(require_docker):
    """Kafka admin client for topic management."""
    from confluent_kafka.admin import AdminClient
    
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    return admin


@pytest.fixture
def test_topic_name():
    """Generate a unique test topic name."""
    return f"test.cdc.{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_test_producer(require_docker):
    """Real Kafka producer for integration tests."""
    from confluent_kafka import Producer
    
    producer = Producer({
        "bootstrap.servers": "127.0.0.1:9092",
        "client.id": "test-producer",
    })
    
    yield producer
    
    producer.flush()


@pytest.fixture
def kafka_test_consumer(require_docker, test_topic_name):
    """Real Kafka consumer for integration tests."""
    from confluent_kafka import Consumer
    
    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": f"test-group-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    
    yield consumer
    
    consumer.close()


# ============================================================
# MinIO Fixtures (Integration Tests)
# ============================================================

@pytest.fixture
def minio_client(require_docker):
    """Real MinIO client for integration tests."""
    from minio import Minio
    
    client = Minio(
        endpoint="127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )
    
    return client


@pytest.fixture
def test_bucket_name():
    """Generate a unique test bucket name."""
    return f"test-bucket-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def test_bucket(minio_client, test_bucket_name):
    """Create a temporary test bucket."""
    # Create bucket
    if not minio_client.bucket_exists(test_bucket_name):
        minio_client.make_bucket(test_bucket_name)
    
    yield test_bucket_name
    
    # Cleanup: Remove all objects and bucket
    try:
        objects = minio_client.list_objects(test_bucket_name, recursive=True)
        for obj in objects:
            minio_client.remove_object(test_bucket_name, obj.object_name)
        minio_client.remove_bucket(test_bucket_name)
    except Exception:
        pass  # Ignore cleanup errors


# ============================================================
# Utility Fixtures
# ============================================================

@pytest.fixture
def temp_env_vars():
    """
    Context manager to temporarily set environment variables.
    
    Usage:
        def test_something(temp_env_vars):
            with temp_env_vars({"MY_VAR": "value"}):
                assert os.environ["MY_VAR"] == "value"
    """
    from contextlib import contextmanager
    
    @contextmanager
    def _temp_env_vars(env_dict: Dict[str, str]):
        original = {}
        for key, value in env_dict.items():
            original[key] = os.environ.get(key)
            os.environ[key] = value
        
        try:
            yield
        finally:
            for key in env_dict:
                if original[key] is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original[key]
    
    return _temp_env_vars


@pytest.fixture
def wait_for_condition():
    """
    Utility to wait for a condition with timeout.
    
    Usage:
        def test_async_operation(wait_for_condition):
            def check_result():
                return some_async_result() == expected
            
            wait_for_condition(check_result, timeout=10)
    """
    def _wait(condition_fn, timeout=30, poll_interval=0.5):
        start = time.time()
        while time.time() - start < timeout:
            if condition_fn():
                return True
            time.sleep(poll_interval)
        raise TimeoutError(f"Condition not met within {timeout}s")
    
    return _wait


# ============================================================
# Integration Test Helper Fixtures
# ============================================================

@pytest.fixture
def kafka_helper(require_docker):
    """Kafka test helper for integration tests."""
    from tests.integration.test_utils import KafkaTestHelper, IntegrationTestConfig
    
    config = IntegrationTestConfig.from_env()
    helper = KafkaTestHelper(config)
    
    yield helper
    
    helper.cleanup()


@pytest.fixture
def source_db_helper(require_docker):
    """Source PostgreSQL test helper for integration tests."""
    from tests.integration.test_utils import PostgresTestHelper
    
    helper = PostgresTestHelper(
        host="127.0.0.1",
        port=5434,
        database="source_db",
        user="cdc_user",
        password="cdc_password",
    )
    
    yield helper
    
    helper.cleanup()
    helper.close()


@pytest.fixture
def target_db_helper(require_docker):
    """Target PostgreSQL test helper for integration tests."""
    from tests.integration.test_utils import PostgresTestHelper
    
    helper = PostgresTestHelper(
        host="127.0.0.1",
        port=5435,
        database="target_db",
        user="target_user",
        password="target_password",
    )
    
    yield helper
    
    helper.cleanup()
    helper.close()


@pytest.fixture
def minio_helper(require_docker):
    """MinIO test helper for integration tests."""
    from tests.integration.test_utils import MinIOTestHelper, IntegrationTestConfig
    
    config = IntegrationTestConfig.from_env()
    helper = MinIOTestHelper(config)
    helper.ensure_bucket()
    
    yield helper


@pytest.fixture
def unique_test_id():
    """Generate unique ID for test isolation."""
    return uuid.uuid4().hex[:8]
