# Phase 6: CDC Consumer - MinIO Archive + Target PostgreSQL Replication

## What We Built

This phase creates the **consumer side** of the CDC pipeline - reading events from Kafka and routing them to multiple destinations.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CDC CONSUMER ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│    Kafka Topics                     Event Router                         │
│    ┌──────────────┐                ┌─────────────────┐                  │
│    │ cdc.source.  │                │                 │                  │
│    │ public.*     │───────────────►│  Deduplication  │                  │
│    └──────────────┘                │       ↓         │                  │
│                                    │   Route Event   │                  │
│                                    │    ↓       ↓    │                  │
│                                    └────┼───────┼────┘                  │
│                                         │       │                       │
│                           ┌─────────────┘       └─────────────┐         │
│                           ↓                                   ↓         │
│                   ┌───────────────┐                   ┌──────────────┐  │
│                   │  MinIO Sink   │                   │ PostgreSQL   │  │
│                   │  (Bronze)     │                   │ Sink         │  │
│                   └───────────────┘                   └──────────────┘  │
│                           │                                   │         │
│                           ↓                                   ↓         │
│                   ┌───────────────┐                   ┌──────────────┐  │
│                   │   cdc-bronze  │                   │ postgres-    │  │
│                   │   bucket      │                   │ target       │  │
│                   └───────────────┘                   └──────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Simple Explanation

**Think of it like a mail distribution center:**
1. **Kafka** is the post office where letters (CDC events) arrive
2. **Consumer** picks up letters from the post office
3. **Deduplication** checks "did we already deliver this?" (avoid duplicates)
4. **Event Router** is the sorting machine - makes copies for each department
5. **MinIO Sink** is the archive room - files everything for record keeping
6. **PostgreSQL Sink** is the replica department - recreates the original document

---

## Technical Deep Dive

### Consumer Group Pattern

```
Kafka Partition 0 ────► Consumer Instance A
Kafka Partition 1 ────► Consumer Instance B  
Kafka Partition 2 ────► Consumer Instance A
Kafka Partition 3 ────► Consumer Instance B
```

**Key Points:**
- All consumers with same `group.id` share partitions
- Each partition is assigned to exactly ONE consumer
- Adding consumers = horizontal scaling
- If a consumer dies, partitions are rebalanced

### At-Least-Once Delivery

```python
# Our pattern (manual commit)
while True:
    msg = consumer.poll()      # 1. Read message
    process(msg)               # 2. Process it
    consumer.commit()          # 3. Mark as done

# If crash between 2 and 3, message is redelivered
# That's why sinks must be IDEMPOTENT!
```

### Idempotent Sink Operations

| Operation | Strategy | SQL Pattern |
|-----------|----------|-------------|
| INSERT | Upsert | `INSERT ... ON CONFLICT DO UPDATE` |
| UPDATE | Conditional | `UPDATE ... WHERE id = ?` |
| DELETE | Ignore if missing | `DELETE ... WHERE id = ?` |

### Fan-Out Architecture

```python
def route(event):
    # Same event goes to MULTIPLE sinks
    minio_sink.write(event)      # Archive
    postgres_sink.write(event)   # Replicate
    
    # Sinks are independent - failure in one doesn't block others
```

---

## Files Created

| File | Purpose |
|------|---------|
| [src/consumer/config.py](../src/consumer/config.py) | All consumer configuration |
| [src/consumer/kafka_consumer.py](../src/consumer/kafka_consumer.py) | Main consumer process |
| [src/consumer/event_router.py](../src/consumer/event_router.py) | Multi-sink dispatcher |
| [src/consumer/minio_sink.py](../src/consumer/minio_sink.py) | Bronze layer archive |
| [src/consumer/postgres_sink.py](../src/consumer/postgres_sink.py) | Target DB replication |
| [src/consumer/deduplication.py](../src/consumer/deduplication.py) | Event ID cache |
| [docker/postgres-target/init.sql](../docker/postgres-target/init.sql) | Target schema setup |

---

## How to Test

### 1. Recreate target database (to pick up new init script)
```bash
docker compose -f docker/docker-compose.yml down postgres-target
docker volume rm cdc-pipeline_postgres_target_data 2>/dev/null || true
docker compose -f docker/docker-compose.yml --env-file .env up -d postgres-target
```

### 2. Verify target schema exists
```bash
docker exec postgres-target psql -U postgres -d target_db -c "\dt"
```
Expected: `customers`, `products`, `orders`, `order_items` tables

### 3. Start the CDC consumer
```bash
python -m src.consumer.kafka_consumer
```

### 4. In another terminal, generate traffic
```bash
python scripts/simulate_traffic.py
```

### 5. Check MinIO for archived events
Open http://localhost:9001 (login: minioadmin/minioadmin)
- Look for `cdc-bronze` bucket
- Events partitioned by `table/year/month/day/hour/`

### 6. Check target PostgreSQL for replicated data
```bash
docker exec postgres-target psql -U postgres -d target_db -c "SELECT * FROM orders LIMIT 5;"
```

---

## Interview Questions

### Q1: Why use consumer groups?
**Answer:** Consumer groups enable horizontal scaling. Kafka assigns partitions across consumers in the same group. Adding consumers increases throughput linearly until we have one consumer per partition.

### Q2: What's the difference between at-least-once and exactly-once?
**Answer:**
- **At-least-once:** Messages may be delivered multiple times. We must handle duplicates (idempotent operations).
- **Exactly-once:** Each message delivered exactly once. Requires Kafka transactions + idempotent producer + consumer idempotence. Higher overhead.

We chose at-least-once + idempotent sinks = "effectively exactly-once" with less complexity.

### Q3: Why archive to MinIO (Bronze layer)?
**Answer:**
1. **Replay:** If processing fails, replay from raw events
2. **Audit:** Immutable history for compliance
3. **Analytics:** Feed Spark/Presto for historical queries
4. **ML:** Training data for anomaly detection
5. **Decoupling:** Bronze doesn't change when downstream changes

### Q4: How do you handle slow consumers?
**Answer:**
1. Add more partitions and consumers (horizontal scale)
2. Batch processing (process N events per commit)
3. Async writes (buffer in memory, flush periodically)
4. Consumer lag monitoring (alert if falling behind)

### Q5: What happens during consumer rebalancing?
**Answer:** When a consumer joins/leaves:
1. All consumers pause
2. Kafka reassigns partitions
3. Consumers resume from last committed offset
4. In-flight messages may be redelivered (duplicates)

### Q6: How do you size the deduplication cache?
**Answer:**
- Estimate: `duplicate_window_size * events_per_second`
- Example: 5 min window × 1000 events/sec = 300,000 events
- Memory: 300K events × ~100 bytes = 30MB
- Use LRU eviction so memory is bounded

### Q7: Why is upsert (ON CONFLICT) idempotent?
**Answer:** If we receive the same INSERT twice:
- First time: Row is inserted
- Second time: ON CONFLICT triggers, row is updated to same values
- Result is identical regardless of how many times we process

---

## Key Concepts Summary

| Concept | What It Means |
|---------|---------------|
| Consumer Group | Multiple consumers sharing work on a topic |
| Partition Assignment | Each partition to exactly one consumer |
| Manual Commit | Control when offsets are committed |
| At-Least-Once | Message delivered 1+ times |
| Idempotent | Same operation repeated = same result |
| Fan-Out | One input, multiple outputs |
| Deduplication | Skip already-processed events |
| Bronze Layer | Raw, immutable data archive |
| Upsert | INSERT or UPDATE if exists |

---

## Next Phase

**Phase 7: Error Handling & Dead Letter Queue**
- Handle malformed events
- Implement retry with exponential backoff
- DLQ for permanently failed events
- Alerting on DLQ growth
