# Phase 14: Performance Testing & Optimization

## Overview

This phase adds comprehensive performance testing and optimization utilities to the CDC pipeline:
- **Benchmarking Framework**: Measure operation throughput and latency
- **Load Generator**: Generate realistic CDC events at configurable rates
- **Metrics Collector**: Track latency histograms and throughput in real-time
- **Optimization Patterns**: Batching, pooling, circuit breaker, rate limiting

---

## Simple Explanation (For Beginners)

### What is Performance Testing?

Think of performance testing like testing a car:
- **Throughput**: How many miles can it drive per hour? (events per second)
- **Latency**: How long does it take to accelerate? (processing time)
- **Resource Usage**: How much fuel does it use? (CPU/memory)

### Why Do We Need It?

1. **Find Bottlenecks**: "Why is processing slow?"
2. **Capacity Planning**: "How many events can we handle?"
3. **Regression Detection**: "Did that change make it slower?"
4. **SLA Verification**: "Do we meet the 99th percentile requirements?"

### Real-World Analogy

Imagine a fast-food restaurant:
```
Customer Order → Kitchen → Food Ready → Served

Performance Metrics:
- Throughput: 100 orders/hour
- Latency: 5 minutes per order
- P99 Latency: 10 minutes (99% of orders within 10 min)
```

---

## Technical Explanation

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Performance Testing Layer                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Benchmark   │  │    Load      │  │   Metrics    │          │
│  │  Framework   │  │  Generator   │  │  Collector   │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         │                 │                 │                   │
│         ▼                 ▼                 ▼                   │
│  ┌──────────────────────────────────────────────────────┐      │
│  │              Optimization Utilities                   │      │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────────────┐│      │
│  │  │ Batch  │ │ Object │ │Circuit │ │ Rate Limiter + ││      │
│  │  │Processor│ │  Pool  │ │Breaker │ │ Retry w/Backoff││      │
│  │  └────────┘ └────────┘ └────────┘ └────────────────┘│      │
│  └──────────────────────────────────────────────────────┘      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Benchmark Framework (`benchmark.py`)

```python
from src.performance import Benchmark, BenchmarkSuite, Timer

# Simple timing
with Timer() as t:
    process_event()
print(f"Took {t.elapsed_ms} ms")

# Benchmarking with statistics
benchmark = Benchmark("serialize_events", warmup_iterations=100)
result = benchmark.run(serialize_function, iterations=1000)

print(result.summary())
# Output:
# Benchmark: serialize_events
#   Iterations: 1000
#   Throughput: 15,432 ops/sec
#   Mean: 0.065 ms
#   P50:  0.062 ms
#   P95:  0.089 ms
#   P99:  0.124 ms

# Benchmark suite for comparing operations
suite = BenchmarkSuite("Serialization Comparison")
suite.add("json", lambda: json.dumps(data))
suite.add("avro", lambda: serializer.serialize(data))
suite.run_all()
print(suite.summary())
```

#### 2. Load Generator (`load_generator.py`)

```python
from src.performance import LoadGenerator, LoadProfile

# Create generator with specific rate
generator = LoadGenerator(
    events_per_second=1000,
    batch_size=100,
    tables=["customers", "orders"],
    table_weights={"customers": 30, "orders": 70},
)

# Generate single event
event = generator.generate_event()

# Generate batch
batch = generator.generate_batch()
print(f"Generated {batch.size} events")

# Stream events with profile
for batch in generator.stream(
    duration_sec=60,
    profile=LoadProfile.RAMP_UP,  # Gradually increase load
):
    process_batch(batch)

# Available load profiles:
# - STEADY: Constant rate
# - RAMP_UP: Gradually increasing (0% → 100%)
# - RAMP_DOWN: Gradually decreasing (100% → 0%)
# - SPIKE: Periodic 3x bursts
# - SINE_WAVE: Oscillating load
# - STEP: Step-function increases
```

#### 3. Metrics Collector (`metrics_collector.py`)

```python
from src.performance import (
    PerformanceMetrics,
    LatencyHistogram,
    ThroughputTracker,
    ResourceMonitor,
)

# Individual components
histogram = LatencyHistogram("processing_time")
with histogram.time():
    process_event()
print(f"P99: {histogram.p99_ms} ms")

throughput = ThroughputTracker("events_processed")
throughput.record(100)  # Processed 100 events
print(f"Rate: {throughput.rate_per_second}/sec")

# Comprehensive metrics
metrics = PerformanceMetrics()
metrics.start_monitoring()

for event in events:
    with metrics.serialization_latency.time():
        serialize(event)
    metrics.events_processed.record()

metrics.stop_monitoring()
metrics.print_report()
```

#### 4. Optimization Utilities (`optimizations.py`)

```python
from src.performance import (
    BatchProcessor,
    ObjectPool,
    CircuitBreaker,
    RateLimiter,
    RetryWithBackoff,
)

# Batch Processor - collect items and process in batches
def bulk_insert(batch):
    db.insert_many(batch)

processor = BatchProcessor(
    batch_size=100,
    flush_interval_sec=5.0,
    process_fn=bulk_insert,
)
for event in events:
    processor.add(event)
processor.flush()  # Process remaining

# Object Pool - reuse expensive objects
def create_connection():
    return database.connect()

pool = ObjectPool(factory=create_connection, max_size=10)
with pool.get() as conn:
    conn.execute(query)

# Circuit Breaker - prevent cascading failures
breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=30,
)

@breaker
def call_external_api():
    return requests.get(url)

# Rate Limiter - control request rate
limiter = RateLimiter(rate=100, burst=10)
for request in requests:
    limiter.acquire()  # Blocks if rate exceeded
    process(request)

# Retry with Backoff - resilient retries
retry = RetryWithBackoff(
    max_attempts=3,
    base_delay=1.0,
    exponential_base=2.0,
)

@retry
def flaky_operation():
    return maybe_fails()
```

---

## Performance Test Results

### Benchmark Results (Typical)

| Operation | Throughput | Mean Latency | P99 Latency |
|-----------|------------|--------------|-------------|
| JSON Serialize | 150,000/sec | 0.006 ms | 0.015 ms |
| JSON Deserialize | 120,000/sec | 0.008 ms | 0.020 ms |
| Pydantic Validate | 80,000/sec | 0.012 ms | 0.030 ms |
| Quality Check | 45,000/sec | 0.022 ms | 0.050 ms |
| Event Generation | 40,000/sec | 0.025 ms | 0.060 ms |

### SLA Compliance

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| JSON P99 | < 5 ms | 0.015 ms | ✅ |
| Quality Check P99 | < 1 ms | 0.050 ms | ✅ |
| Throughput | > 10,000/sec | 45,000/sec | ✅ |

---

## Project Structure

```
src/performance/
├── __init__.py              # Module exports
├── benchmark.py             # Timer, Benchmark, BenchmarkSuite, LatencyTracker
├── load_generator.py        # LoadGenerator, LoadProfile, EventBatch
├── metrics_collector.py     # LatencyHistogram, ThroughputTracker, ResourceMonitor
└── optimizations.py         # BatchProcessor, ObjectPool, CircuitBreaker, etc.

tests/performance/
├── conftest.py              # Shared fixtures
├── test_benchmark.py        # 26 tests for benchmarking
├── test_load_generator.py   # 23 tests for load generation
├── test_metrics_collector.py # 19 tests for metrics
├── test_optimizations.py    # 30 tests for optimization utilities
└── test_pipeline_benchmarks.py # 15 pipeline benchmarks
```

---

## Interview Questions & Answers

### Q1: How do you measure throughput vs latency?

**Simple Answer:**
- **Throughput**: Count operations per second
- **Latency**: Measure time for each operation

**Technical Answer:**
```python
# Throughput: events/second using windowed counting
tracker = ThroughputTracker(window_size_sec=60)
for event in events:
    process(event)
    tracker.record()
print(f"{tracker.rate_per_second} events/sec")

# Latency: microsecond-precision timing
histogram = LatencyHistogram()
with histogram.time():
    process(event)
print(f"P99: {histogram.p99_ms} ms")
```

### Q2: Why use percentiles instead of averages?

**Simple Answer:**
Averages hide outliers. If 99 requests take 1ms and 1 takes 100ms, the average is 2ms but users see 100ms delays.

**Technical Answer:**
```
P50 (median): Half of requests faster than this
P95: 95% faster (SLA threshold)
P99: 99% faster (tail latency)
P999: 99.9% faster (worst case)

Example Distribution:
- Mean: 10ms (misleading)
- P50: 5ms
- P99: 50ms (reality for 1% of users)
```

### Q3: What is a circuit breaker?

**Simple Answer:**
Like a home circuit breaker - if too many failures, stop trying and give the system time to recover.

**Technical Answer:**
```python
States:
CLOSED → Normal operation
         │ (failures > threshold)
         ▼
OPEN   → Reject all calls, wait for recovery_timeout
         │ (timeout expires)
         ▼
HALF_OPEN → Allow one call to test
            │ (success)
            ▼
CLOSED   → Back to normal
```

### Q4: How do you handle backpressure?

**Simple Answer:**
If the consumer can't keep up, slow down the producer or drop messages.

**Technical Answer:**
```python
# Bounded buffer with rejection
processor = BatchProcessor(
    batch_size=100,
    max_buffer_size=10000,  # Backpressure limit
)

if not processor.add(event):  # Buffer full
    dlq.send(event)  # Send to DLQ or drop

# Rate limiting
limiter = RateLimiter(rate=1000)
for event in events:
    limiter.acquire()  # Blocks if too fast
    process(event)
```

### Q5: When would you use object pooling?

**Simple Answer:**
When creating objects is expensive (database connections, file handles).

**Technical Answer:**
```python
# Without pooling: O(n) connection time
for query in queries:
    conn = create_connection()  # Expensive!
    conn.execute(query)
    conn.close()

# With pooling: O(1) amortized
pool = ObjectPool(create_connection, max_size=10)
for query in queries:
    with pool.get() as conn:  # Reuses connection
        conn.execute(query)
```

### Q6: How do you benchmark fairly?

**Answer:**
1. **Warmup**: Let JIT/caches stabilize
2. **Isolation**: No other processes competing
3. **Repetition**: Run many iterations for statistics
4. **Realistic data**: Use production-like inputs
5. **Multiple metrics**: Mean, median, percentiles, stdev

```python
benchmark = Benchmark(
    name="serialize",
    warmup_iterations=100,  # Critical for JIT
)
result = benchmark.run(serialize, iterations=1000)
# Reports mean, median, p50, p95, p99, stdev
```

---

## Key Takeaways

1. **Benchmark Everything**: Measure before optimizing
2. **Use Percentiles**: P99 matters more than mean
3. **Batch Operations**: Amortize overhead
4. **Pool Resources**: Reuse expensive objects
5. **Handle Failures**: Circuit breakers prevent cascades
6. **Rate Limit**: Protect downstream systems

---

## Next Steps

- **Phase 15**: Schema Evolution & Versioning
- **Phase 16**: Multi-Region Deployment
- **Phase 17**: Security & Authentication
- **Phase 18**: Monitoring Dashboards
- **Phase 19**: CI/CD Pipeline
- **Phase 20**: Production Deployment
