# Phase 4: Kafka Topic Design and Event Schema

## 🎯 Objective of This Phase

**In simple language:**
Now that we have a source database generating changes and Kafka running, we need to create "mailboxes" (topics) in Kafka where CDC events will be delivered. We also need to define what a CDC event LOOKS like — its structure/format. Think of it like designing an envelope: where does it go (topic), what's written on the outside (headers), and what's inside (payload).

**In technical language:**
This phase designs the Kafka topic topology, naming conventions, partitioning strategy, and defines the CDC event schema using Pydantic models. We'll create topics explicitly (no auto-creation), configure retention policies, and implement a Python script to create topics programmatically. We'll also validate the schema by producing and consuming test messages.

---

## 📚 Concepts You Need to Understand First

### 1. What is a Kafka Topic?

**Simple explanation:**
A topic is like a named channel or mailbox. All CDC events from the `orders` table go to the `orders` topic. All events from `customers` go to the `customers` topic. Producers send messages TO topics. Consumers read messages FROM topics.

**Technical explanation:**
A Kafka topic is a category/feed name to which records are published. Topics are divided into partitions — ordered, immutable sequences of records. Each record within a partition is assigned a sequential ID called an offset. Topics are the fundamental unit of organization in Kafka.

### 2. What is a Partition?

**Simple explanation:**
A partition is like a lane on a highway. More lanes = more cars can travel simultaneously = more throughput. Within one lane, cars maintain order (first in, first out). But across lanes, there's no guaranteed order. We put related events in the same lane (partition) to keep them in order.

**Technical explanation:**
Partitions enable parallelism — multiple consumers can read from different partitions simultaneously. Within a partition, messages are strictly ordered by offset. A partition key determines which partition a message goes to (hash of key % num_partitions). For CDC, we use the primary key as the partition key so all changes to the same row go to the same partition, maintaining per-entity order.

### 3. What is a Partition Key?

**Simple explanation:**
The partition key answers: "Which lane should this event go to?" For our CDC events, we use the row's primary key (e.g., `customer_id=5`). This guarantees ALL changes to customer #5 go to the same partition and arrive in order: INSERT → UPDATE → UPDATE → DELETE.

**Technical explanation:**
Kafka hashes the partition key and mods by partition count to determine the target partition: `partition = hash(key) % num_partitions`. Using the entity's primary key ensures all operations on that entity are co-located in a single partition, guaranteeing causal ordering. This is CRITICAL for CDC — without it, an UPDATE could arrive before its INSERT.

### 4. What is a Consumer Group?

**Simple explanation:**
A consumer group is a team of workers sharing the workload. If a topic has 3 partitions and your group has 3 consumers, each consumer handles 1 partition. If one consumer dies, the others pick up its partition. The group ID is the team name.

**Technical explanation:**
A consumer group is a set of consumers that cooperatively consume from a topic. Kafka assigns each partition to exactly one consumer within a group (but a consumer can handle multiple partitions). Consumer group offsets are committed to Kafka, tracking read progress. Rebalancing redistributes partitions when consumers join/leave the group.

### 5. What is a Dead Letter Queue (DLQ)?

**Simple explanation:**
When a message is "poisonous" — it can't be processed no matter how many times you retry (maybe it's malformed, or causes an error) — you don't want it blocking the pipeline. You move it to a special "reject bin" topic called the Dead Letter Queue. Someone can investigate later.

**Technical explanation:**
A DLQ is a separate Kafka topic where messages that fail processing after maximum retries are routed. This prevents poison messages from blocking the consumer. DLQ messages include the original payload plus error metadata (reason, timestamp, retry count). Operations teams monitor DLQ depth as a health metric.

### 6. What is an Event Schema?

**Simple explanation:**
A schema defines the STRUCTURE of a message — what fields it contains, what types they are, which are required. It's like a form template: "This CDC event must have: operation type, table name, timestamp, and the changed data." Without a schema, consumers don't know how to read messages.

**Technical explanation:**
The event schema is a formal contract between producers and consumers. It defines the message format (field names, types, nullability). We use Pydantic models for validation at the application level. In production, you'd also register schemas in a Schema Registry (Avro/Protobuf) for cross-language compatibility and evolution rules.

---

## 📁 Files We'll Create in This Phase

```
cdc-pipeline/
├── scripts/
│   └── create_kafka_topics.py    # Creates all required Kafka topics
├── src/
│   └── schemas/
│       └── cdc_event.py          # Pydantic models for CDC events
└── docs/
    └── phase-04-kafka-topics.md  # This file
```

---

## 🏗️ Topic Naming Convention

We follow the widely-adopted convention: `<prefix>.<database>.<schema>.<table>`

```
Topic Naming Pattern:
  cdc.<source>.<schema>.<table>

Our Topics:
  cdc.source.public.customers      ← All changes to customers table
  cdc.source.public.products       ← All changes to products table
  cdc.source.public.orders         ← All changes to orders table
  cdc.source.public.order_items    ← All changes to order_items table
  cdc.dead_letter_queue            ← Failed/poison messages
  cdc.schema_changes               ← (Future) DDL change events
```

### Why this convention?

| Segment | Purpose | Example |
|---------|---------|---------|
| `cdc` | Identifies this as a CDC event (vs. application events) | Namespace |
| `source` | Which database instance (useful if you have multiple) | source, analytics, legacy |
| `public` | PostgreSQL schema name | public, sales, billing |
| `customers` | Table name | Exact match to source table |

### Partitioning Strategy:

| Topic | Partitions | Partition Key | Why |
|-------|-----------|---------------|-----|
| `cdc.source.public.customers` | 3 | `customer.id` | Per-entity ordering |
| `cdc.source.public.products` | 3 | `product.id` | Per-entity ordering |
| `cdc.source.public.orders` | 6 | `order.id` | Higher volume, more parallelism |
| `cdc.source.public.order_items` | 6 | `order_id` | Co-locate with parent order |
| `cdc.dead_letter_queue` | 1 | None | Low volume, ordering not critical |

---

## 📋 CDC Event Schema Design

### Event Envelope (wrapper around the actual data):

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_timestamp": "2026-06-15T10:30:00.123456Z",
  "source": {
    "database": "source_db",
    "schema": "public",
    "table": "orders",
    "transaction_id": 12345,
    "lsn": "0/16B3748"
  },
  "operation": "UPDATE",
  "before": {
    "id": 1,
    "customer_id": 5,
    "status": "pending",
    "total_amount": 99.99,
    "updated_at": "2026-06-15T10:00:00Z"
  },
  "after": {
    "id": 1,
    "customer_id": 5,
    "status": "confirmed",
    "total_amount": 99.99,
    "updated_at": "2026-06-15T10:30:00Z"
  }
}
```

### Field Explanations:

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | UUID | Unique identifier for deduplication |
| `event_timestamp` | ISO 8601 | When the change happened |
| `source.database` | string | Source database name |
| `source.schema` | string | PostgreSQL schema |
| `source.table` | string | Table that changed |
| `source.transaction_id` | int | PostgreSQL transaction ID (for grouping) |
| `source.lsn` | string | Log Sequence Number (WAL position) |
| `operation` | enum | INSERT, UPDATE, DELETE |
| `before` | object/null | Row state BEFORE change (null for INSERT) |
| `after` | object/null | Row state AFTER change (null for DELETE) |

### Operation Types:

| Operation | `before` | `after` |
|-----------|----------|---------|
| INSERT | `null` | full row data |
| UPDATE | old row values | new row values |
| DELETE | full row data | `null` |

---

## 🧪 Testing and Validation

```bash
# 1. Verify topics were created
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# Expected: All 5 topics listed

# 2. Verify topic details
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cdc.source.public.orders
# Expected: Shows 6 partitions, replication factor 1

# 3. Test produce a message
python scripts/create_kafka_topics.py --test
# Expected: Produces test CDC event, consumes it back, validates schema

# 4. Verify consumer group mechanics
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
# Expected: Shows test consumer group if test was run
```

---

## ⚠️ Common Mistakes and Debugging Tips

| Problem | Cause | Fix |
|---------|-------|-----|
| "Topic already exists" | Running create script twice | Use `--if-not-exists` flag or check before creating |
| Messages going to wrong partition | Wrong partition key | Ensure key is the entity's primary key (as bytes/string) |
| Consumer not receiving messages | Wrong group ID or topic name | Double-check topic name spelling and consumer group |
| "Leader not available" | Topic just created, metadata propagating | Wait 2-3 seconds after creation before producing |
| Messages out of order | Using random or no partition key | Always set partition key = primary key for CDC |
| Serialization error | Schema mismatch between producer and consumer | Validate with Pydantic before producing |

---

## 🎤 Interview Questions for This Phase

### Beginner Level:

1. **Q:** Why do we create one topic per table instead of one topic for all CDC events?
   **A:** Separate topics enable independent consumer scaling, different retention policies per table, easier access control (grant access to specific tables), and consumers can subscribe only to tables they care about without filtering.

2. **Q:** What happens if you produce to a topic that doesn't exist and auto-create is disabled?
   **A:** The producer receives a `TopicNotFoundError`. We disable auto-create intentionally — in production, topics should be created with explicit partition counts and configs through a governed process, not accidentally by a misconfigured producer.

3. **Q:** Why is the partition key important for CDC events?
   **A:** The partition key ensures all changes to the same entity (row) go to the same partition. Within a partition, order is guaranteed. Without a key, changes to the same row could be distributed across partitions and arrive out of order (UPDATE before INSERT).

### Intermediate Level:

4. **Q:** How do you decide the number of partitions for a topic?
   **A:** Partitions = max number of parallel consumers you expect. Consider: (a) expected throughput ÷ throughput per consumer, (b) target latency requirements, (c) you can increase partitions later but NEVER decrease them. Start conservative — over-partitioning wastes resources and complicates ordering.

5. **Q:** What's the difference between message key and partition key?
   **A:** They're the same in Kafka. The message key is hashed to determine the partition (`hash(key) % num_partitions`). It also appears in the message headers so consumers can use it for processing logic (e.g., grouping by entity ID).

6. **Q:** How would you handle a situation where order_items must be processed AFTER the parent order?
   **A:** Use the same partition key (`order_id`) for both `orders` and `order_items` topics. Since they share the key, related events go to the same partition number. The consumer processes events sequentially within a partition, so if the producer sends order first and items second, they arrive in order.

### Advanced Level:

7. **Q:** What happens to message ordering if you increase the partition count of a running topic?
   **A:** Existing messages stay in their current partitions. But NEW messages with the same key may hash to a DIFFERENT partition (since `hash(key) % new_count` changes). This breaks per-entity ordering for in-flight data. Solution: create a new topic with the desired partition count and migrate consumers.

8. **Q:** Design a topic strategy for a CDC pipeline processing 100 tables with varying throughput.
   **A:** High-throughput tables (orders, events): dedicated topics with 12+ partitions. Medium tables (customers, products): dedicated topics with 3-6 partitions. Low-throughput reference tables (countries, categories): single topic with 1 partition (grouped). Use topic naming convention for discoverability. Apply different retention per tier.

---

## ✅ Phase 4 Completion Checklist

- [ ] All 5 Kafka topics created successfully
- [ ] Topics have correct partition counts
- [ ] Topic naming convention is consistent
- [ ] CDC event schema (Pydantic model) is defined and validated
- [ ] Test message successfully produced and consumed
- [ ] Schema handles INSERT, UPDATE, and DELETE events correctly
- [ ] Dead letter queue topic exists
- [ ] I understand partition keys and why ordering matters
- [ ] I understand consumer groups
- [ ] I can explain the topic naming convention in an interview

---

## 🚀 What's Next (Phase 5 Preview)

In Phase 5, we will:
- Connect to PostgreSQL's logical replication slot
- Read WAL changes using Python
- Transform WAL output into our CDC event schema
- Publish real CDC events to Kafka topics

**Reply "Phase 4 complete" when your checklist is done.**
