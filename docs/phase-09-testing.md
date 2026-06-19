# Phase 9: End-to-End Testing

## 📖 Overview

This phase adds a comprehensive testing framework for the CDC pipeline, covering unit tests, integration tests, and end-to-end tests.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         TESTING PYRAMID                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│                          ┌─────────┐                                    │
│                          │   E2E   │   Slow, expensive, few             │
│                          │  Tests  │   Full pipeline flow               │
│                         ┌┴─────────┴┐                                   │
│                         │Integration │  Medium speed                    │
│                         │   Tests    │  Component interactions          │
│                        ┌┴────────────┴┐                                 │
│                        │  Unit Tests   │ Fast, cheap, many              │
│                        │               │ Single functions               │
│                        └───────────────┘                                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Simple Explanation

**What is Testing?**
Testing is like checking your homework before submitting:
- **Unit Tests**: Check each answer individually (2+2=4? ✓)
- **Integration Tests**: Check that related answers are consistent
- **E2E Tests**: Have someone else solve the whole problem to verify

**Why Test?**
- Catch bugs before they reach production
- Confidence to refactor code
- Documentation of expected behavior
- Faster development (fewer manual tests)

---

## 🔧 Technical Explanation

### Test Categories

| Type | Speed | Dependencies | Purpose |
|------|-------|--------------|---------|
| **Unit** | ~1ms/test | None (mocked) | Test single functions/classes |
| **Integration** | ~100ms/test | Docker services | Test component interactions |
| **E2E** | ~1s/test | Full pipeline | Test complete flows |

### Pytest Markers

```python
@pytest.mark.unit        # Fast, no dependencies
@pytest.mark.integration # Requires Docker
@pytest.mark.e2e         # Full pipeline
@pytest.mark.slow        # Long-running tests
```

### Key Testing Patterns

1. **Fixtures**: Reusable test setup
2. **Mocking**: Fake external dependencies
3. **Parametrize**: Run same test with different inputs
4. **Markers**: Categorize and filter tests

---

## 📁 Files Created

```
tests/
├── conftest.py                    # Shared fixtures
├── pytest.ini                     # Pytest configuration
├── unit/
│   ├── test_cdc_event.py         # CDC event schema tests
│   └── test_deduplication.py     # Deduplication cache tests
├── integration/
│   ├── test_postgres_sink.py     # PostgreSQL sink tests
│   └── test_minio_sink.py        # MinIO sink tests
└── e2e/
    └── test_pipeline_flow.py     # Full pipeline tests
```

---

## 🧪 Running Tests

### Install Test Dependencies
```bash
pip install pytest pytest-cov pytest-asyncio
```

### Run All Tests
```bash
# From cdc-pipeline directory
pytest
```

### Run by Category
```bash
# Unit tests only (fast, no Docker needed)
pytest tests/unit -v

# Integration tests (requires Docker)
pytest tests/integration -v

# E2E tests (requires full pipeline)
pytest tests/e2e -v -m e2e

# Skip slow tests
pytest -m "not slow"
```

### Run with Coverage
```bash
pytest --cov=src --cov-report=html
# Open htmlcov/index.html in browser
```

### Run Specific Test
```bash
# Run single test file
pytest tests/unit/test_deduplication.py -v

# Run single test function
pytest tests/unit/test_deduplication.py::TestDeduplicationCache::test_new_event_not_duplicate -v
```

---

## 📊 Test Coverage

| Component | Tests | Coverage |
|-----------|-------|----------|
| CDC Event Schema | 12 | ~95% |
| Deduplication Cache | 15 | ~100% |
| PostgreSQL Sink | 8 | ~80% |
| MinIO Sink | 10 | ~80% |
| Pipeline Flow (E2E) | 8 | ~60% |

---

## 🔍 Key Test Cases

### Unit Tests

| Test | What It Verifies |
|------|------------------|
| `test_create_insert_event` | INSERT event has correct structure |
| `test_event_serialization` | Events serialize to valid JSON |
| `test_cache_eviction` | LRU eviction works correctly |
| `test_thread_safety` | Cache handles concurrent access |

### Integration Tests

| Test | What It Verifies |
|------|------------------|
| `test_insert_customer` | INSERT events write to target DB |
| `test_insert_is_idempotent` | Duplicate events don't cause errors |
| `test_events_persisted_as_jsonl` | MinIO files are valid JSON Lines |
| `test_separate_buffers_per_table` | Each table has its own buffer |

### E2E Tests

| Test | What It Verifies |
|------|------------------|
| `test_source_and_target_match` | Data consistency across pipeline |
| `test_consumer_handles_invalid_message` | Invalid messages don't crash system |
| `test_batch_insert_performance` | Pipeline handles load |

---

## 🧩 Fixtures Reference

### Data Fixtures

| Fixture | Returns | Use For |
|---------|---------|---------|
| `sample_insert_event` | CDCEvent | Testing INSERT handling |
| `sample_update_event` | CDCEvent | Testing UPDATE handling |
| `sample_delete_event` | CDCEvent | Testing DELETE handling |
| `sample_events_batch` | List[CDCEvent] | Testing batch processing |

### Mock Fixtures

| Fixture | Returns | Use For |
|---------|---------|---------|
| `mock_kafka_producer` | MagicMock | Unit tests without Kafka |
| `mock_kafka_consumer` | MagicMock | Unit tests without Kafka |
| `mock_minio_client` | MagicMock | Unit tests without MinIO |
| `mock_postgres_connection` | MagicMock | Unit tests without DB |

### Integration Fixtures

| Fixture | Returns | Use For |
|---------|---------|---------|
| `source_db_connection` | psycopg2 connection | Source DB tests |
| `target_db_connection` | psycopg2 connection | Target DB tests |
| `minio_client` | Minio client | MinIO tests |
| `kafka_test_producer` | Kafka Producer | Kafka tests |

---

## 💡 Interview Questions

### Q: What's the testing pyramid?
**A:** A strategy for test distribution:
- **Bottom (Unit)**: Many fast, cheap tests for individual functions
- **Middle (Integration)**: Fewer tests for component interactions
- **Top (E2E)**: Few slow, expensive tests for full flows

### Q: Why mock external dependencies in unit tests?
**A:** 
1. **Speed**: No network calls, tests run in milliseconds
2. **Isolation**: Test only the code, not external systems
3. **Reliability**: No flaky failures from network issues
4. **Control**: Can simulate error conditions

### Q: How do you handle test data isolation?
**A:** 
1. Use unique IDs per test (`uuid.uuid4()`)
2. Rollback transactions after tests
3. Clean up created resources in fixtures
4. Use separate test databases/buckets

### Q: What makes a good test?
**A:** FIRST principles:
- **Fast**: Quick to run
- **Isolated**: No dependencies between tests
- **Repeatable**: Same result every time
- **Self-validating**: Clear pass/fail
- **Timely**: Written with the code

### Q: How do you test async/streaming systems?
**A:**
1. Use `wait_for_condition` helpers with timeouts
2. Polling with exponential backoff
3. Async test frameworks (pytest-asyncio)
4. Message assertions on output topics

---

## 🔜 Next Steps (Phase 10)

Phase 10 will add **Schema Registry** with:
- Avro schema definitions
- Schema evolution and compatibility
- Confluent Schema Registry integration

---

## 📚 References

- [Pytest Documentation](https://docs.pytest.org/)
- [Testing Best Practices](https://martinfowler.com/articles/practical-test-pyramid.html)
- [Pytest Fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [Test-Driven Development](https://testdriven.io/blog/modern-tdd/)
