# Phase 12: Pipeline Integration - DLQ & Quality Checks

## What We Built

Integrated data quality validation with the consumer pipeline, routing failed events to a **Dead Letter Queue (DLQ)** for later review and replay.

```
Kafka Message
     │
     ▼
┌─────────────────┐
│ Deserialize     │──── Error ────▶ DLQ
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Quality Check   │──── Fail ─────▶ DLQ
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Event Router    │──── Error ────▶ DLQ
└─────────────────┘
     │
     ▼
   Success ✓
```

---

## Simple Explanation

### What is a Dead Letter Queue (DLQ)?

Think of a DLQ like the **"return to sender"** pile at a post office:

- When a letter can't be delivered, it goes to a special area
- Someone reviews it to figure out what went wrong
- If it can be fixed, they retry delivery
- If not, they contact the sender

In our pipeline:
- **Letters** = CDC events
- **Failed delivery** = Quality check failure, database error, etc.
- **DLQ** = A separate Kafka topic where bad events go
- **Review** = Engineers inspect and fix issues
- **Retry** = Replay events after fixing the problem

### Why Do We Need a DLQ?

| Without DLQ | With DLQ |
|-------------|----------|
| Bad events are dropped | Bad events are saved |
| Data is lost forever | Data can be recovered |
| Silent failures | Visible failures |
| Hard to debug | Easy to investigate |
| No learning | Pattern analysis possible |

### The Processing Pipeline

```
1. DESERIALIZE ─────────────────────────────────────────────────────────
   │
   │ Can we read this message?
   │
   ├── NO ──▶ DLQ (reason: deser_error)
   │          "This message is garbage, can't parse it"
   │
   └── YES ─▶ Continue

2. QUALITY CHECK ───────────────────────────────────────────────────────
   │
   │ Does this event pass our validation rules?
   │
   ├── NO ──▶ DLQ (reason: quality_failure)
   │          "Event is missing required fields or has bad data"
   │
   └── YES ─▶ Continue

3. ROUTE TO SINKS ──────────────────────────────────────────────────────
   │
   │ Can we write to MinIO and PostgreSQL?
   │
   ├── NO ──▶ DLQ (reason: sink_failure)
   │          "Database or storage is down"
   │
   └── YES ─▶ SUCCESS!
```

---

## Technical Explanation

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        QualityAwareProcessor                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │ JSON Parser  │────▶│ Quality      │────▶│ Event        │────▶ Success   │
│  │              │     │ Checker      │     │ Router       │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│         │                    │                    │                         │
│         │                    │                    │                         │
│         ▼                    ▼                    ▼                         │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │                     DLQ Handler                               │          │
│  │  • send_quality_failure()                                     │          │
│  │  • send_sink_failure()                                        │          │
│  │  • send_deserialization_error()                               │          │
│  └──────────────────────────────────────────────────────────────┘          │
│                              │                                              │
│                              ▼                                              │
│                   ┌──────────────────┐                                     │
│                   │ Kafka DLQ Topic  │                                     │
│                   │ cdc.dead_letter  │                                     │
│                   └──────────────────┘                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### DLQ Entry Structure

Every failed event includes:

```python
@dataclass
class DLQEntry:
    # Original event data (for replay)
    original_event: dict
    original_topic: str
    original_partition: int
    original_offset: int
    
    # Why it failed (for debugging)
    failure_reason: FailureReason
    error_message: str
    error_details: Optional[dict] = None
    
    # When it failed (for tracking)
    failed_at: str  # ISO timestamp
    retry_count: int = 0
    consumer_id: str = ""
    
    # Quality-specific (if quality failure)
    quality_report: Optional[dict] = None
```

### Failure Reasons

| Reason | Description | Fix |
|--------|-------------|-----|
| `quality_failure` | Event failed validation rules | Fix source data or adjust rules |
| `sink_failure` | Database/MinIO write failed | Check infrastructure |
| `deser_error` | Couldn't parse message | Fix producer serialization |
| `schema_mismatch` | Schema incompatibility | Update consumer schema |
| `transform_error` | Transformation failed | Fix transformation logic |
| `timeout` | Processing too slow | Scale or optimize |

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cdc_dlq_events_total` | Counter | Total events sent to DLQ |
| `cdc_dlq_events_by_reason_total` | Counter | Events by failure reason |

---

## Code Reference

### Using the Processor

```python
from src.consumer import QualityAwareProcessor

# Create processor (uses defaults)
processor = QualityAwareProcessor()

# Or with custom components
processor = QualityAwareProcessor(
    router=my_router,
    dlq_handler=my_dlq,
    enable_quality_checks=True,
)

# Process a Kafka message
result = processor.process_message(
    raw_value=message.value(),
    topic=message.topic(),
    partition=message.partition(),
    offset=message.offset(),
)

if result.success:
    print(f"Processed: {result.event_id}")
else:
    print(f"Failed at {result.stage}: {result.error_message}")
```

### Registering Table-Specific Checkers

```python
from src.consumer import QualityAwareProcessor
from src.quality import QualityChecker, RequiredFieldRule, RangeRule

# Create processor
processor = QualityAwareProcessor()

# Create custom checker for orders table
order_checker = (
    QualityChecker(name="orders")
    .add_rule(RequiredFieldRule("order_id"))
    .add_rule(RequiredFieldRule("customer_id"))
    .add_rule(RangeRule("total", min_value=0))
)

# Register for orders table
processor.register_checker("orders", order_checker)

# Now orders get custom validation, others use default
```

### Using the DLQ Handler Directly

```python
from src.consumer import DLQHandler, FailureReason

# Create handler
dlq = DLQHandler(
    dlq_topic="cdc.dead_letter_queue",
    consumer_id="my-consumer",
)

# Send quality failure
dlq.send_quality_failure(
    event=event_dict,
    quality_report=report.to_dict(),
    topic="source-topic",
    partition=0,
    offset=100,
)

# Send sink failure
dlq.send_sink_failure(
    event=event_dict,
    sink_name="postgres",
    error=exception,
    retry_count=3,
    topic="source-topic",
    partition=0,
    offset=100,
)

# Get stats
print(dlq.get_stats())
# {'total_sent': 5, 'by_reason': {'quality_failure': 3, 'sink_failure': 2}}
```

### Processing Results

```python
from src.consumer import ProcessingResult

# Success
result = ProcessingResult(
    success=True,
    event_id="evt-123",
    stage="complete",
)

# Failure
result = ProcessingResult(
    success=False,
    event_id="evt-456",
    stage="quality",
    error_message="Missing required field: customer_id",
)

# Use in code
if not result.success:
    if result.stage == "quality":
        # Quality issue - maybe alert data team
        pass
    elif result.stage == "routing":
        # Infrastructure issue - maybe alert ops
        pass
```

---

## File Structure

```
src/consumer/
├── __init__.py           # Updated with new exports
├── dlq_handler.py        # NEW: DLQ handling logic
├── event_processor.py    # NEW: Quality-aware processing
├── event_router.py       # Existing: Routes to sinks
├── kafka_consumer.py     # Existing: Kafka consumer
├── deduplication.py      # Existing: Dedup cache
├── minio_sink.py         # Existing: MinIO sink
├── postgres_sink.py      # Existing: PostgreSQL sink
└── config.py             # Existing: Configuration

src/metrics/
└── pipeline_metrics.py   # Added DLQ metrics

tests/unit/
└── test_dlq_processor.py # NEW: 18 tests for DLQ/processor
```

---

## Testing

```bash
# Run DLQ and processor tests
python -m pytest tests/unit/test_dlq_processor.py -v

# Output: 18 passed
```

### Test Coverage

| Category | Tests |
|----------|-------|
| FailureReason enum | 2 |
| DLQEntry dataclass | 3 |
| DLQHandler | 5 |
| QualityAwareProcessor | 6 |
| ProcessingResult | 2 |

---

## Integration Example

### Full Consumer with Quality Checks

```python
from confluent_kafka import Consumer
from src.consumer import QualityAwareProcessor
from src.quality import create_customer_checker, create_order_checker

# Create processor
processor = QualityAwareProcessor()

# Register table-specific checkers
processor.register_checker("customers", create_customer_checker())
processor.register_checker("orders", create_order_checker())

# Create Kafka consumer
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "cdc-consumer-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["cdc.source.public.customers", "cdc.source.public.orders"])

# Process loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        result = processor.process_message(
            raw_value=msg.value(),
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )
        
        if result.success:
            consumer.commit(msg)  # Commit on success
        else:
            print(f"Event {result.event_id} failed: {result.error_message}")
            # Event already in DLQ, commit to move forward
            consumer.commit(msg)

finally:
    processor.close()
    consumer.close()
```

---

## Interview Questions & Answers

### Q1: What is a Dead Letter Queue and why is it important?

**Simple Answer:**
A Dead Letter Queue (DLQ) is a special queue for messages that can't be processed. Instead of losing bad messages, we save them for later review. It's like a "lost and found" for your data.

**Technical Answer:**
A DLQ is a messaging pattern that captures messages that fail processing. Key benefits:

1. **Data Preservation**: No data loss even when processing fails
2. **Debugging**: Failed messages contain error context for investigation
3. **Retry Capability**: Messages can be replayed after fixing issues
4. **Isolation**: Bad messages don't block good ones
5. **Monitoring**: Track failure rates and patterns

In CDC pipelines, DLQ is crucial because:
- Source data may have unexpected formats
- Downstream systems may be temporarily unavailable
- Schema evolution can cause compatibility issues

---

### Q2: How do you handle different types of failures in a pipeline?

**Simple Answer:**
We categorize failures by type (quality, infrastructure, parsing) and handle each appropriately. Quality issues need data fixes. Infrastructure issues need ops attention.

**Technical Answer:**
Failure categorization with appropriate responses:

| Failure Type | Detection | Response | Escalation |
|--------------|-----------|----------|------------|
| Deserialization | JSON parse fails | DLQ + alert | Investigate producer |
| Quality | Validation rules fail | DLQ + metrics | Data team review |
| Sink (transient) | Retry exhausted | DLQ + backpressure | Ops alert |
| Sink (permanent) | Config error | DLQ + halt | Immediate action |

```python
if failure_reason == FailureReason.QUALITY_FAILURE:
    # Data issue - log, metrics, continue processing
    alert_data_team(event, report)
elif failure_reason == FailureReason.SINK_FAILURE:
    # Infrastructure issue - may need circuit breaker
    if is_transient(error):
        schedule_retry(event)
    else:
        alert_ops_team(event, error)
```

---

### Q3: How do you replay events from a DLQ?

**Simple Answer:**
Read events from the DLQ topic, fix what caused them to fail, then send them back through the main pipeline. Like returning a fixed product to the assembly line.

**Technical Answer:**
DLQ replay strategies:

1. **Manual Replay**:
   ```bash
   # Read DLQ, fix events, republish to source topic
   kafka-console-consumer --topic cdc.dlq | fix_events.py | kafka-console-producer --topic source
   ```

2. **Automated Replay Service**:
   ```python
   class DLQReplayService:
       def replay_batch(self, filter_reason=None, dry_run=False):
           for entry in self.consume_dlq():
               if filter_reason and entry.failure_reason != filter_reason:
                   continue
               if dry_run:
                   self.validate(entry)
               else:
                   self.republish(entry.original_event)
   ```

3. **Time-Based Replay**:
   - Replay events from a specific time window
   - Useful after infrastructure recovery

Best practices:
- Add replay metadata (attempt count, original_failed_at)
- Set max retry limits to prevent infinite loops
- Monitor replay success rates

---

### Q4: How do you monitor DLQ health in production?

**Simple Answer:**
Track how many events go to DLQ, why they fail, and how long they've been there. Alert when numbers spike or events age.

**Technical Answer:**
Key DLQ metrics and alerts:

```python
# Metrics
dlq_events_total         # Total DLQ entries
dlq_events_by_reason     # Breakdown by failure type
dlq_lag                  # Unprocessed DLQ messages
dlq_oldest_message_age   # Time since oldest entry

# Alerts
- dlq_events_total rate > 10/min → Warning
- dlq_events_by_reason{reason="sink_failure"} spike → Investigate infra
- dlq_lag > 1000 → DLQ consumer falling behind
- dlq_oldest_message_age > 24h → Events aging out
```

Grafana dashboard panels:
1. DLQ rate over time (by reason)
2. Top failing event types
3. DLQ consumer lag
4. Average time-to-resolution

---

### Q5: What's the difference between at-least-once and exactly-once with DLQ?

**Simple Answer:**
At-least-once means events might be processed twice. Exactly-once means each event is processed only once. DLQ helps achieve exactly-once by tracking which events succeeded.

**Technical Answer:**
Delivery semantics with DLQ:

**At-Least-Once**:
- Commit offset after successful processing
- On failure, event stays in source topic for retry
- May process duplicates after restart
- DLQ catches permanent failures

**Exactly-Once** (with idempotency):
- Combine deduplication with DLQ
- Track event_id in dedup cache
- Commit offsets atomically with writes
- DLQ events get unique "replay_id"

```python
# Exactly-once flow
def process(event):
    if dedup_cache.is_duplicate(event.event_id):
        return True  # Already processed
    
    try:
        with transaction:
            sink.write(event)
            dedup_cache.add(event.event_id)
            consumer.commit()
        return True
    except PermanentError:
        dlq.send(event)
        consumer.commit()  # Don't retry permanent failures
        return False
```

---

### Q6: How do you prevent DLQ from filling up?

**Simple Answer:**
Set retention policies (delete after N days), process events regularly, and fix root causes so fewer events fail.

**Technical Answer:**
DLQ management strategies:

1. **Retention Policy**:
   ```
   # Kafka topic config
   retention.ms=604800000  # 7 days
   retention.bytes=10737418240  # 10 GB
   ```

2. **Automated Processing**:
   - Scheduled DLQ consumer that retries transient failures
   - Alert on permanent failures for manual review

3. **Root Cause Analysis**:
   ```python
   # Aggregate failures by type
   SELECT 
       failure_reason,
       error_message,
       COUNT(*) as count
   FROM dlq_events
   WHERE failed_at > NOW() - INTERVAL '24 hours'
   GROUP BY 1, 2
   ORDER BY 3 DESC
   ```

4. **Circuit Breaker**:
   - If DLQ rate exceeds threshold, pause processing
   - Prevents cascading failures

---

### Q7: How do you test DLQ behavior?

**Simple Answer:**
Unit tests with mocked Kafka, integration tests with real Kafka, and chaos testing where we intentionally break things.

**Technical Answer:**
Multi-level DLQ testing:

**Unit Tests** (mocked):
```python
def test_quality_failure_goes_to_dlq(mock_dlq):
    processor = QualityAwareProcessor(dlq_handler=mock_dlq)
    
    bad_event = {"operation": "INVALID"}
    processor.process_message(json.dumps(bad_event).encode())
    
    mock_dlq.send_quality_failure.assert_called_once()
```

**Integration Tests** (real Kafka):
```python
def test_dlq_message_format():
    # Send bad event to source topic
    producer.produce("source", invalid_json)
    
    # Verify it appears in DLQ with correct format
    dlq_msg = dlq_consumer.poll(timeout=10)
    entry = json.loads(dlq_msg.value())
    
    assert entry["failure_reason"] == "deser_error"
    assert "original_event" in entry
```

**Chaos Testing**:
- Kill database mid-write → verify DLQ capture
- Corrupt messages → verify deserialization errors
- Exceed rate limits → verify backpressure

---

### Q8: How do you prioritize DLQ events for replay?

**Simple Answer:**
Replay the most important events first. Critical customer data before analytics. Recent events before old ones.

**Technical Answer:**
DLQ prioritization strategies:

1. **By Business Impact**:
   ```python
   PRIORITY = {
       "customers": 1,  # High - affects customer experience
       "orders": 1,     # High - revenue impact
       "analytics": 3,  # Low - can wait
   }
   
   def get_priority(dlq_entry):
       table = dlq_entry.original_event.get("source", {}).get("table")
       return PRIORITY.get(table, 2)
   ```

2. **By Failure Reason**:
   - Transient failures (sink_failure) → retry immediately
   - Quality failures → batch for review
   - Schema mismatches → fix schema first

3. **By Age**:
   - Prevent stale data by processing oldest first
   - But also set max age limits

4. **By Retry Count**:
   - First-time failures get priority
   - Multi-failure events may need manual intervention

---

### Q9: What happens if the DLQ itself fails?

**Simple Answer:**
We log the error prominently and alert immediately. The original event may be lost, which is why we have multiple safeguards.

**Technical Answer:**
DLQ failure handling:

```python
def _send(self, entry: DLQEntry) -> None:
    try:
        self._producer.produce(...)
    except Exception as e:
        # CRITICAL: DLQ send failed
        logger.critical(
            f"DLQ SEND FAILED | event_id={entry.original_event.get('event_id')} | "
            f"error={e} | EVENT MAY BE LOST"
        )
        
        # Fallback strategies:
        # 1. Write to local file (last resort)
        self._write_to_emergency_file(entry)
        
        # 2. Increment critical alert counter
        DLQ_SEND_FAILURES.inc()
        
        # 3. Circuit breaker - pause processing
        if self._failure_count > 10:
            raise CircuitBreakerOpen()
```

Safeguards:
1. DLQ topic with high replication (3+)
2. Local file fallback
3. Circuit breaker to prevent cascading failures
4. Immediate alerting

---

### Q10: How does DLQ fit into a data mesh architecture?

**Simple Answer:**
Each data domain owns its own DLQ. They're responsible for their data quality and failures. Central platform provides the DLQ infrastructure.

**Technical Answer:**
Data mesh with DLQ:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Platform Team                                 │
│  • DLQ infrastructure (Kafka, monitoring)                       │
│  • Standard DLQ format and tooling                              │
│  • Central alerting and dashboards                              │
└─────────────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Customer     │    │ Order        │    │ Inventory    │
│ Domain       │    │ Domain       │    │ Domain       │
│              │    │              │    │              │
│ DLQ: cdc.dlq │    │ DLQ: cdc.dlq │    │ DLQ: cdc.dlq │
│  .customers  │    │  .orders     │    │  .inventory  │
│              │    │              │    │              │
│ Owner: Team A│    │ Owner: Team B│    │ Owner: Team C│
└──────────────┘    └──────────────┘    └──────────────┘
```

Each domain:
- Owns data quality rules for their data products
- Monitors their DLQ
- Responsible for replay and fixes

Platform provides:
- Consistent DLQ schema
- Shared monitoring tools
- Cross-domain analytics

---

## Summary

Phase 12 adds **pipeline integration** with DLQ and quality checks:

| Component | Purpose |
|-----------|---------|
| `DLQHandler` | Routes failed events to Kafka DLQ topic |
| `DLQEntry` | Structured format for DLQ messages |
| `FailureReason` | Categorizes why events failed |
| `QualityAwareProcessor` | Orchestrates quality checks + routing |
| Metrics | DLQ event counts and reasons |

**80 unit tests passing** (18 new for DLQ/processor).

Next: **Phase 13 - End-to-End Testing** - Full integration tests with Docker.
