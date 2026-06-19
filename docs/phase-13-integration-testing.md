# Phase 13: End-to-End Testing

## What We Built

A comprehensive **integration testing framework** for the CDC pipeline, including:
- Test utilities for Kafka, PostgreSQL, and MinIO
- Full CDC flow tests
- Failure scenario tests
- End-to-end scenario tests

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Testing Pyramid                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                         ┌─────────┐                                         │
│                         │   E2E   │  Full pipeline tests                    │
│                         │  Tests  │  (requires all services)                │
│                        ─┴─────────┴─                                        │
│                                                                              │
│                    ┌───────────────────┐                                    │
│                    │   Integration     │  Service-level tests              │
│                    │      Tests        │  (Kafka, DB, MinIO)               │
│                   ─┴───────────────────┴─                                   │
│                                                                              │
│              ┌───────────────────────────────┐                              │
│              │         Unit Tests            │  Component tests             │
│              │   (80 tests, fast, mocked)    │  (no external deps)          │
│             ─┴───────────────────────────────┴─                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Simple Explanation

### Why Do We Need Integration Tests?

Think of it like testing a car:

| Test Type | Car Analogy | What We Test |
|-----------|-------------|--------------|
| **Unit Tests** | Test the engine in isolation | Individual functions work |
| **Integration Tests** | Test engine + transmission together | Components work together |
| **E2E Tests** | Take the car for a drive | Whole system works |

Unit tests are fast but don't catch problems between components. Integration tests catch those issues.

### What Can Go Wrong Between Components?

1. **Format Mismatch**: Producer sends JSON, consumer expects Avro
2. **Network Issues**: Kafka is down, database timeout
3. **Schema Changes**: Source table changed, consumer doesn't know
4. **Ordering Problems**: Events arrive out of order
5. **Resource Exhaustion**: Too many connections, full disk

### Our Testing Strategy

```
1. MOCK FOR SPEED
   Unit tests → Mock Kafka, DB, MinIO
   ✓ Fast (milliseconds)
   ✓ No Docker needed
   ✗ Doesn't catch real issues

2. REAL FOR CONFIDENCE
   Integration tests → Real Kafka, DB, MinIO
   ✓ Catches real issues
   ✓ Tests actual behavior
   ✗ Slower (seconds)
   ✗ Requires Docker

3. FULL FLOW FOR PROOF
   E2E tests → Source DB → Kafka → Consumer → Sinks
   ✓ Proves system works end-to-end
   ✗ Slowest (minutes)
   ✗ Complex to set up
```

---

## Technical Explanation

### Test Utilities Architecture

```
tests/
├── conftest.py                 # Shared fixtures
├── integration/
│   ├── test_utils.py           # Helper classes
│   │   ├── TestConfig          # Configuration
│   │   ├── KafkaTestHelper     # Kafka operations
│   │   ├── PostgresTestHelper  # Database operations
│   │   └── MinIOTestHelper     # Object storage operations
│   ├── test_full_flow.py       # Full CDC flow tests
│   └── test_failure_scenarios.py # Error handling tests
└── unit/
    └── *.py                    # Unit tests (80 tests)
```

### Test Helpers

#### KafkaTestHelper
```python
class KafkaTestHelper:
    """Kafka operations for tests."""
    
    def create_topic(self, name, num_partitions=1)
    def delete_topic(self, name)
    def produce(self, topic, value, key=None)
    def produce_cdc_event(self, topic, operation, table, data)
    def consume_messages(self, topic, count, timeout)
    def consume_dlq_messages(self, dlq_topic, count)
    def cleanup()
```

#### PostgresTestHelper
```python
class PostgresTestHelper:
    """Database operations for tests."""
    
    def execute(self, query, params)
    def execute_commit(self, query, params)
    def insert_customer(self, first_name, last_name, email)
    def insert_order(self, customer_id, total)
    def get_customer(self, customer_id)
    def cleanup()
```

#### MinIOTestHelper
```python
class MinIOTestHelper:
    """Object storage operations for tests."""
    
    def ensure_bucket(self, bucket)
    def list_objects(self, bucket, prefix)
    def get_object(self, key, bucket)
    def get_object_json(self, key, bucket)
    def count_objects(self, bucket, prefix)
    def cleanup(self, prefix)
```

### Test Markers

```python
@pytest.mark.unit        # Fast, no external deps
@pytest.mark.integration # Requires Docker services
@pytest.mark.e2e         # Full pipeline test
@pytest.mark.slow        # Long-running test
```

### Running Tests

```bash
# Unit tests only (fast, no Docker)
pytest tests/unit/ -v

# Integration tests (requires Docker)
pytest tests/integration/ -v -m integration

# E2E tests (requires all services + running producer)
pytest tests/e2e/ -v -m e2e

# Skip slow tests
pytest -m "not slow"

# Run all tests
pytest tests/ -v
```

---

## Test Categories

### 1. Kafka Flow Tests

| Test | What It Verifies |
|------|------------------|
| `test_produce_and_consume_cdc_event` | Basic produce/consume works |
| `test_produce_batch_events` | Multiple events handled |
| `test_message_ordering_within_partition` | Order preserved |

### 2. Quality-Aware Processing Tests

| Test | What It Verifies |
|------|------------------|
| `test_valid_event_passes_quality_check` | Good events pass |
| `test_invalid_operation_fails_quality_check` | Bad operations rejected |
| `test_malformed_json_fails_deserialization` | Parse errors handled |

### 3. DLQ Handling Tests

| Test | What It Verifies |
|------|------------------|
| `test_quality_failure_produces_dlq_message` | Quality failures go to DLQ |
| `test_sink_failure_produces_dlq_message` | Sink failures go to DLQ |
| `test_dlq_preserves_original_offset` | Metadata preserved |

### 4. Failure Scenario Tests

| Test | What It Verifies |
|------|------------------|
| `test_missing_required_field` | Required field validation |
| `test_invalid_json` | JSON parse error handling |
| `test_router_exception_goes_to_dlq` | Routing errors captured |
| `test_continue_after_quality_failure` | Pipeline continues |

### 5. Edge Case Tests

| Test | What It Verifies |
|------|------------------|
| `test_very_large_event` | Large events handled |
| `test_unicode_in_events` | Unicode characters work |
| `test_null_values_in_event` | Null handling correct |

---

## File Structure

```
tests/
├── conftest.py               # Updated with new fixtures
├── integration/
│   ├── test_utils.py         # NEW: Helper classes
│   ├── test_full_flow.py     # NEW: Full CDC flow tests
│   ├── test_failure_scenarios.py # NEW: Error handling tests
│   ├── test_minio_sink.py    # Existing: MinIO tests
│   └── test_postgres_sink.py # Existing: Postgres tests
├── unit/
│   ├── test_cdc_event.py
│   ├── test_data_quality.py
│   ├── test_deduplication.py
│   └── test_dlq_processor.py
└── e2e/
    └── test_pipeline_flow.py # Existing: E2E tests
```

---

## Running Integration Tests

### Prerequisites

```bash
# Start Docker services
cd docker && docker-compose up -d

# Verify services are running
docker-compose ps

# Expected output:
# kafka            Up
# postgres-source  Up
# postgres-target  Up
# minio            Up
```

### Run Tests

```bash
# All unit tests (no Docker needed)
pytest tests/unit/ -v
# Output: 80 passed

# Integration tests (Docker required)
pytest tests/integration/ -v -m integration

# Skip integration tests if Docker not available
pytest tests/ -v -m "not integration"
```

### Test Configuration

Tests use environment variables or defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Kafka connection |
| `SOURCE_PG_HOST` | `127.0.0.1` | Source DB host |
| `SOURCE_PG_PORT` | `5434` | Source DB port |
| `TARGET_PG_PORT` | `5435` | Target DB port |
| `MINIO_ENDPOINT` | `127.0.0.1:9000` | MinIO endpoint |

---

## Interview Questions & Answers

### Q1: What's the difference between unit, integration, and E2E tests?

**Simple Answer:**
- **Unit**: Test one piece in isolation (fast, many of them)
- **Integration**: Test pieces working together (medium speed)
- **E2E**: Test the whole system (slow, few of them)

**Technical Answer:**

| Aspect | Unit | Integration | E2E |
|--------|------|-------------|-----|
| **Scope** | Single function/class | Multiple components | Full system |
| **Dependencies** | Mocked | Real (subset) | All real |
| **Speed** | Milliseconds | Seconds | Minutes |
| **Count** | Many (80+) | Moderate (20-50) | Few (5-10) |
| **Maintenance** | Low | Medium | High |
| **Confidence** | Low (isolated) | Medium | High |

Testing pyramid principle:
```
      /\       E2E (few, slow, high confidence)
     /  \
    /----\     Integration (moderate)
   /------\
  /--------\   Unit (many, fast, isolated)
```

---

### Q2: How do you handle test data cleanup?

**Simple Answer:**
We track what we create and delete it after the test. Like cleaning up your desk after working on a project.

**Technical Answer:**
Multiple strategies:

1. **Tracking Pattern**:
```python
class PostgresTestHelper:
    def __init__(self):
        self._test_records = []  # Track (table, id)
    
    def insert_customer(self, ...):
        # ... insert ...
        self._test_records.append(("customers", customer_id))
    
    def cleanup(self):
        for table, record_id in reversed(self._test_records):
            execute(f"DELETE FROM {table} WHERE id = %s", (record_id,))
```

2. **Transaction Rollback**:
```python
@pytest.fixture
def db_connection():
    conn = connect()
    conn.autocommit = False
    yield conn
    conn.rollback()  # Undo all changes
    conn.close()
```

3. **Unique Test Namespacing**:
```python
@pytest.fixture
def unique_test_id():
    return uuid.uuid4().hex[:8]

def test_something(unique_test_id):
    topic = f"test.{unique_test_id}"  # Won't conflict
```

---

### Q3: How do you test asynchronous operations?

**Simple Answer:**
We wait for results with a timeout. If it doesn't happen in time, the test fails.

**Technical Answer:**
Polling with timeout:

```python
def wait_for_condition(condition, timeout=30, poll_interval=0.5):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(poll_interval)
    raise TimeoutError("Condition not met")

# Usage
def test_async_operation():
    producer.send(event)
    
    def message_arrived():
        messages = consumer.poll()
        return len(messages) > 0
    
    wait_for_condition(message_arrived, timeout=10)
```

Key principles:
- Always use timeouts (tests shouldn't hang forever)
- Use reasonable poll intervals (not too fast = CPU waste)
- Return early on success (don't wait full timeout)

---

### Q4: How do you handle flaky tests?

**Simple Answer:**
Flaky tests fail sometimes for no good reason. We find the cause (usually timing) and fix it, or we mark it for retry.

**Technical Answer:**
Flaky test causes and solutions:

| Cause | Solution |
|-------|----------|
| **Timing issues** | Add proper waits/assertions |
| **Resource contention** | Use unique names/IDs per test |
| **External service state** | Reset state before test |
| **Order dependency** | Make tests independent |
| **Non-deterministic data** | Use fixed seeds/factories |

```python
# Bad: Flaky timing
def test_flaky():
    start_process()
    time.sleep(1)  # Hope it's ready
    assert check_result()

# Good: Explicit waiting
def test_reliable(wait_for_condition):
    start_process()
    wait_for_condition(lambda: is_ready(), timeout=10)
    assert check_result()
```

---

### Q5: How do you test error handling in distributed systems?

**Simple Answer:**
We intentionally cause errors (kill services, send bad data) and verify the system handles them correctly.

**Technical Answer:**
Fault injection strategies:

1. **Application-level mocking**:
```python
def test_database_failure():
    mock_db = Mock()
    mock_db.write.side_effect = ConnectionError("DB down")
    
    processor = Processor(db=mock_db, dlq=real_dlq)
    result = processor.process(event)
    
    assert result.success is False
    assert_event_in_dlq(event)
```

2. **Network-level faults** (Chaos Engineering):
```bash
# Kill database container
docker stop postgres-source

# Run test
pytest test_db_failure.py

# Restore
docker start postgres-source
```

3. **Invalid data injection**:
```python
def test_invalid_json():
    producer.send(topic, b"{invalid json")
    
    dlq_messages = consume_dlq()
    assert dlq_messages[0]["failure_reason"] == "deser_error"
```

---

### Q6: What makes a good integration test?

**Simple Answer:**
A good integration test is isolated (doesn't affect other tests), reliable (doesn't flake), and tests real behavior (not mocked).

**Technical Answer:**
Integration test principles:

1. **Isolation**: Each test independent
   ```python
   @pytest.fixture
   def unique_topic(unique_test_id):
       return f"test.{unique_test_id}"  # Each test gets unique topic
   ```

2. **Idempotent**: Can run multiple times
   ```python
   def test_can_run_twice():
       # Cleanup first
       delete_test_data()
       # Then test
       create_and_verify()
   ```

3. **Fast enough**: Use parallelization
   ```bash
   pytest -n 4  # Run 4 tests in parallel
   ```

4. **Clear assertions**: Test one thing well
   ```python
   # Bad: Too many assertions
   def test_everything():
       assert result.success
       assert result.count == 5
       assert result.timing < 1.0
       # ...
   
   # Good: Focused
   def test_produces_message():
       produce(event)
       messages = consume()
       assert len(messages) == 1
   ```

---

### Q7: How do you test Kafka message ordering?

**Simple Answer:**
Kafka guarantees order within a partition. We send messages with the same key and verify they arrive in order.

**Technical Answer:**
```python
def test_message_ordering_within_partition():
    # Same key = same partition = guaranteed order
    for i in range(10):
        producer.produce(
            topic="test",
            key="same-key",  # Forces same partition
            value={"sequence": i},
        )
    
    messages = consume(count=10)
    sequences = [m["sequence"] for m in messages]
    
    assert sequences == list(range(10)), "Order must be preserved"
```

Important notes:
- Order only guaranteed within a partition
- Same key → same partition (if using default partitioner)
- Cross-partition order is not guaranteed

---

### Q8: How do you test DLQ behavior?

**Simple Answer:**
We send bad events, then check the DLQ to see if they arrived with the right error information.

**Technical Answer:**
DLQ testing strategy:

```python
def test_dlq_captures_quality_failure():
    # 1. Send invalid event
    processor.process(invalid_event)
    
    # 2. Consume from DLQ
    dlq_messages = kafka.consume_dlq(count=1, timeout=10)
    
    # 3. Verify structure
    entry = dlq_messages[0]
    assert entry["failure_reason"] == "quality_failure"
    assert entry["original_event"] == invalid_event
    assert "failed_at" in entry
    
    # 4. Verify metadata for replay
    assert entry["original_topic"] == source_topic
    assert entry["original_offset"] == original_offset
```

DLQ test categories:
- Entry structure (all fields present)
- Failure categorization (correct reason)
- Original data preservation (for replay)
- Timestamp format (ISO 8601)

---

### Q9: How do you handle test environment setup/teardown?

**Simple Answer:**
We use Docker Compose to start everything before tests and fixtures to clean up after each test.

**Technical Answer:**
Multi-level setup:

1. **Environment level** (CI/CD):
```yaml
# .github/workflows/test.yml
steps:
  - uses: docker/compose-action@v1
    with:
      compose-file: docker/docker-compose.yml
  - run: pytest tests/
```

2. **Session level** (pytest fixtures):
```python
@pytest.fixture(scope="session")
def docker_services_available():
    """Check once at session start."""
    return check_services()
```

3. **Test level** (cleanup after each):
```python
@pytest.fixture
def kafka_helper(require_docker):
    helper = KafkaTestHelper()
    yield helper
    helper.cleanup()  # Delete created topics
```

Fixture scopes:
- `session`: Once per test run
- `module`: Once per test file
- `function` (default): Once per test

---

### Q10: How do you measure integration test coverage?

**Simple Answer:**
We track which code paths are executed during tests. Integration tests focus on component interaction paths, not line coverage.

**Technical Answer:**
Integration test coverage metrics:

1. **Code Coverage** (lines executed):
```bash
pytest --cov=src tests/integration/
# Reports: 85% lines covered
```

2. **Component Coverage** (interactions tested):
```
✓ Producer → Kafka
✓ Kafka → Consumer
✓ Consumer → Quality Check
✓ Quality Check → DLQ
✓ Consumer → MinIO
✓ Consumer → PostgreSQL
```

3. **Scenario Coverage** (use cases tested):
```
✓ Happy path (INSERT, UPDATE, DELETE)
✓ Quality failures
✓ Deserialization errors
✓ Sink failures
✓ Recovery after failures
✓ High volume processing
```

Key insight: Integration tests prioritize scenario coverage over line coverage. A test that covers the error path is more valuable than one that covers more lines.

---

## Summary

Phase 13 adds **comprehensive integration testing**:

| Component | Purpose |
|-----------|---------|
| `test_utils.py` | Helper classes for Kafka, DB, MinIO |
| `test_full_flow.py` | Full CDC flow tests |
| `test_failure_scenarios.py` | Error handling tests |
| Updated `conftest.py` | New fixtures for helpers |

### Test Counts

| Category | Count |
|----------|-------|
| Unit Tests | 80 |
| Integration Tests | ~30 (new) |
| E2E Tests | ~5 |

### Running Tests

```bash
# Unit tests (no Docker)
pytest tests/unit/ -v

# Integration tests (requires Docker)
pytest tests/integration/ -v -m integration

# All tests
pytest tests/ -v
```

Next: **Phase 14 - Performance Testing & Optimization**
