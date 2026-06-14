# Phase 1: Project Architecture and System Design

## 🎯 Objective of This Phase

**In simple language:**
Before writing a single line of code, we need to understand WHAT we are building, WHY each piece exists, and HOW they all connect together. Think of this like creating a blueprint before constructing a building. No architect starts laying bricks without a plan.

**In technical language:**
This phase establishes the system architecture, data flow diagrams, component responsibilities, communication patterns, and failure boundaries for a production-grade CDC pipeline. We will define the logical and physical architecture, identify data contracts between components, and establish the project structure.

---

## 📚 Concepts You Need to Understand First

### 1. What is Change Data Capture (CDC)?

**Simple explanation:**
Imagine you have a notebook (database) where you write orders. Every time you write, erase, or modify something, a hidden camera (CDC) records that change. Later, someone else can watch the recording and replicate exactly what happened in their own notebook.

**Technical explanation:**
CDC is a technique that identifies and captures changes (INSERT, UPDATE, DELETE) made to a database, and delivers those changes in real-time to downstream systems. Instead of querying the entire table repeatedly (polling), CDC reads the database's internal change log.

### 2. What is WAL (Write-Ahead Log)?

**Simple explanation:**
Before PostgreSQL actually saves your data, it first writes a "note to self" in a journal called WAL. This journal says "I'm about to do X." If the power goes out mid-save, PostgreSQL reads this journal on restart to recover. We're going to READ this journal to detect changes.

**Technical explanation:**
WAL (Write-Ahead Log) is PostgreSQL's mechanism for ensuring data durability. Every transaction is first written to the WAL before being applied to the actual data files. PostgreSQL supports "logical decoding" of WAL, which translates the binary WAL entries into a readable stream of row-level changes. This is what we tap into for CDC.

### 3. What is Event Streaming (Kafka)?

**Simple explanation:**
Think of Kafka as a super-fast post office. Producers (senders) drop messages into mailboxes (topics). Consumers (receivers) pick up messages from those mailboxes. The messages are stored in order and can be read multiple times by different consumers.

**Technical explanation:**
Apache Kafka is a distributed event streaming platform. It provides durable, ordered, partitioned log storage. Producers publish events to topics, and consumers read from topics at their own pace. Kafka retains messages for a configurable period, enabling replay and multiple consumer patterns.

### 4. What is Object Storage (MinIO)?

**Simple explanation:**
MinIO is like a giant filing cabinet in the cloud (but running on your machine). You throw files into "buckets" (drawers) and retrieve them by name. It's cheap, unlimited storage for raw data you might need later.

**Technical explanation:**
MinIO is an S3-compatible object storage system. In our architecture, it serves as the raw event archive — the "bronze layer" where every CDC event is preserved in its original form. This enables replay, auditing, and reprocessing without touching the source database.

### 5. What is the Medallion Architecture (Bronze/Silver/Gold)?

**Simple explanation:**
- **Bronze:** Raw, unprocessed data (exactly as captured)
- **Silver:** Cleaned, deduplicated, validated data
- **Gold:** Business-ready aggregations and metrics

Think of it like cooking: Bronze = raw ingredients from the market, Silver = washed, chopped, and prepped ingredients, Gold = the finished dish served to customers.

**Technical explanation:**
The medallion architecture is a data design pattern that organizes data in a lakehouse through progressive quality layers. Each layer adds structure, validation, and business logic. This enables data replay from any layer and clear separation of concerns.

### 6. What is Orchestration (Airflow)?

**Simple explanation:**
Airflow is like a task scheduler on steroids. It runs "do this, then do that, then do this other thing" automatically, on a schedule, and tells you if something failed.

**Technical explanation:**
Apache Airflow is a workflow orchestration platform that defines, schedules, and monitors directed acyclic graphs (DAGs) of tasks. In our pipeline, it coordinates batch operations like reconciliation checks, dbt runs, and snapshot management.

### 7. What is Data Transformation (dbt)?

**Simple explanation:**
dbt lets you write SQL to transform raw data into useful tables. It's like Excel formulas but for databases — and it tracks all the dependencies between your formulas automatically.

**Technical explanation:**
dbt (data build tool) is a transformation framework that enables analytics engineers to transform data in their warehouse using SQL SELECT statements. It handles dependency management, testing, documentation, and incremental materialization.

### 8. What is Observability (Prometheus + Grafana)?

**Simple explanation:**
- **Prometheus** = A system that constantly asks "how are you?" to your services and records the answers (metrics like "processed 500 events/second")
- **Grafana** = A beautiful dashboard that visualizes those recorded answers in real-time charts

**Technical explanation:**
Prometheus is a time-series metrics database that scrapes metrics endpoints. Grafana is a visualization platform that queries Prometheus and renders dashboards. Together they provide pipeline observability: throughput, latency, error rates, consumer lag.

---

## 🏗️ System Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CDC PIPELINE ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐         ┌──────────────┐         ┌──────────────────┐
    │  PostgreSQL  │── WAL ─▶│    Kafka     │── msg ─▶│ Python Consumer  │
    │   (Source)   │  logs   │   Cluster    │         │   (Processor)    │
    └──────────────┘         └──────────────┘         └──────────────────┘
           │                        │                    │       │       │
           │                        │                    │       │       │
           ▼                        ▼                    ▼       ▼       ▼
    ┌──────────────┐         ┌──────────────┐    ┌──────┐ ┌─────┐ ┌────────┐
    │   Airflow    │         │  Schema      │    │MinIO │ │ PG  │ │Promethe│
    │(Orchestrator)│         │  Registry    │    │(Arch)│ │(Tgt)│ │  us    │
    └──────────────┘         └──────────────┘    └──────┘ └─────┘ └────────┘
           │                                                 │         │
           ▼                                                 ▼         ▼
    ┌──────────────┐                                  ┌──────────┐ ┌───────┐
    │     dbt      │─────────────────────────────────▶│ Superset │ │Grafana│
    │(Transforms)  │                                  │(BI Tool) │ │(Ops)  │
    └──────────────┘                                  └──────────┘ └───────┘
```

### Detailed Component Interaction

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  SOURCE LAYER                                                           │
│  ┌─────────────────────────────────────────┐                           │
│  │  PostgreSQL Source Database              │                           │
│  │  ┌─────────┐  ┌─────────┐  ┌────────┐  │                           │
│  │  │ orders  │  │customers│  │products│  │                           │
│  │  └─────────┘  └─────────┘  └────────┘  │                           │
│  │           │                              │                           │
│  │           ▼ (Logical Replication Slot)   │                           │
│  │  ┌─────────────────────┐                │                           │
│  │  │  WAL Logical Decode │                │                           │
│  │  │  (wal2json plugin)  │                │                           │
│  │  └─────────────────────┘                │                           │
│  └──────────────┬──────────────────────────┘                           │
│                 │                                                        │
│  ───────────────┼────────────────────────────────────────────────────── │
│                 ▼                                                        │
│  STREAMING LAYER                                                        │
│  ┌─────────────────────────────────────────┐                           │
│  │  Apache Kafka                            │                           │
│  │  ┌───────────────────────────────────┐  │                           │
│  │  │ Topic: cdc.source.public.orders   │  │                           │
│  │  │ Topic: cdc.source.public.customers│  │                           │
│  │  │ Topic: cdc.source.public.products │  │                           │
│  │  │ Topic: cdc.dead-letter-queue      │  │                           │
│  │  └───────────────────────────────────┘  │                           │
│  └──────────────┬──────────────────────────┘                           │
│                 │                                                        │
│  ───────────────┼────────────────────────────────────────────────────── │
│                 ▼                                                        │
│  PROCESSING LAYER                                                       │
│  ┌─────────────────────────────────────────┐                           │
│  │  Python CDC Consumer                     │                           │
│  │  ┌───────────┐ ┌──────────┐ ┌────────┐ │                           │
│  │  │ Deseriali-│ │ Transform│ │ Route  │ │                           │
│  │  │   zer     │ │  & Valid │ │        │ │                           │
│  │  └─────┬─────┘ └────┬─────┘ └───┬────┘ │                           │
│  │        │             │            │      │                           │
│  └────────┼─────────────┼────────────┼──────┘                           │
│           │             │            │                                   │
│  ─────────┼─────────────┼────────────┼──────────────────────────────── │
│           ▼             ▼            ▼                                   │
│  STORAGE LAYER                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────────┐                   │
│  │   MinIO    │  │ PostgreSQL │  │  Prometheus    │                   │
│  │  (Bronze)  │  │  (Target)  │  │  (Metrics)    │                   │
│  │            │  │  (Silver)  │  │               │                   │
│  └────────────┘  └─────┬──────┘  └───────┬────────┘                   │
│                        │                  │                              │
│  ──────────────────────┼──────────────────┼──────────────────────────── │
│                        ▼                  ▼                              │
│  ANALYTICS LAYER                                                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────────┐                   │
│  │    dbt     │  │  Superset  │  │    Grafana     │                   │
│  │   (Gold)   │  │    (BI)    │  │  (Monitoring)  │                   │
│  └────────────┘  └────────────┘  └────────────────┘                   │
│                                                                         │
│  ORCHESTRATION LAYER (spans all above)                                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Apache Airflow                                 │   │
│  │  DAG: initial_snapshot | DAG: reconciliation | DAG: dbt_run     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🧩 Component Responsibilities

| Component | Role | Why This Tool? |
|-----------|------|----------------|
| **PostgreSQL (Source)** | OLTP database simulating a real application | Industry standard; supports logical replication natively |
| **WAL + Logical Decoding** | Captures row-level changes without impacting source performance | Zero-impact CDC; no triggers, no polling, no application changes needed |
| **Apache Kafka** | Event bus that decouples source from consumers | Durable, ordered, replayable; industry standard for event streaming |
| **Python Consumer** | Processes CDC events, applies business logic, routes to sinks | Flexible; rich ecosystem; good for custom processing logic |
| **MinIO** | Archives raw CDC events (Bronze layer) | S3-compatible; enables replay from any point in time; cheap storage |
| **PostgreSQL (Target)** | Analytics-ready replica (Silver layer) | Same engine for consistency; good for complex queries |
| **dbt** | Transforms Silver → Gold layer | SQL-based; version controlled; dependency management; testing built-in |
| **Apache Airflow** | Schedules batch operations | DAG-based; rich scheduling; failure handling; monitoring UI |
| **Apache Superset** | Business intelligence dashboards | Open source; SQL-native; connects directly to PostgreSQL |
| **Prometheus** | Collects pipeline metrics | Pull-based; efficient time-series; alerting rules |
| **Grafana** | Visualizes operational metrics | Beautiful dashboards; Prometheus integration; alerting |
| **Docker** | Containerizes all services | Reproducibility; isolation; easy local development |

---

## 📊 Data Flow Explanation

### What happens when someone inserts a row?

```
Step 1: Application writes → INSERT INTO orders (customer_id, amount) VALUES (1, 99.99)
Step 2: PostgreSQL writes to WAL → Transaction log entry recorded
Step 3: Logical Decoding reads WAL → Converts binary WAL to JSON change event
Step 4: Python Producer sends to Kafka → {"op": "INSERT", "table": "orders", "data": {...}}
Step 5: Kafka stores the event → Persisted to topic partition, assigned offset
Step 6: Python Consumer reads from Kafka → Picks up event at next poll
Step 7: Consumer routes to 3 destinations:
        ├── MinIO: Raw JSON archived (Bronze)
        ├── Target PostgreSQL: Row inserted/updated/deleted (Silver)
        └── Prometheus: Metric counter incremented
Step 8: Airflow triggers dbt → Transforms Silver tables into Gold aggregations
Step 9: Superset queries Gold → Dashboards update
Step 10: Grafana shows metrics → Pipeline health visible
```

### What happens on UPDATE?

```
Source: UPDATE orders SET amount = 149.99 WHERE id = 1
Event:  {"op": "UPDATE", "table": "orders", "before": {"amount": 99.99}, "after": {"amount": 149.99}}
Target: UPDATE applied using primary key match
```

### What happens on DELETE?

```
Source: DELETE FROM orders WHERE id = 1
Event:  {"op": "DELETE", "table": "orders", "before": {"id": 1, "amount": 149.99}}
Target: Row marked as deleted (soft delete) or removed (hard delete) based on strategy
```

---

## 📁 Project Folder Structure

```
cdc-pipeline/
├── docker/                          # Docker configurations
│   ├── docker-compose.yml           # All services definition
│   ├── postgres-source/             # Source DB custom config
│   │   ├── Dockerfile
│   │   ├── postgresql.conf
│   │   └── init.sql
│   ├── postgres-target/             # Target DB custom config
│   │   ├── Dockerfile
│   │   └── init.sql
│   └── kafka/                       # Kafka custom config
│       └── server.properties
│
├── src/                             # Application source code
│   ├── producer/                    # CDC event producer (WAL reader)
│   │   ├── __init__.py
│   │   ├── wal_reader.py           # Reads PostgreSQL WAL
│   │   ├── kafka_producer.py       # Publishes to Kafka
│   │   └── config.py
│   │
│   ├── consumer/                    # CDC event consumer
│   │   ├── __init__.py
│   │   ├── kafka_consumer.py       # Reads from Kafka
│   │   ├── event_router.py         # Routes to MinIO/PG/metrics
│   │   ├── minio_sink.py           # Archives to MinIO
│   │   ├── postgres_sink.py        # Replicates to target PG
│   │   ├── deduplication.py        # Exactly-once logic
│   │   └── config.py
│   │
│   ├── schemas/                     # Event schemas
│   │   ├── cdc_event.py            # Pydantic models
│   │   └── avro/                   # Avro schema files
│   │
│   └── metrics/                     # Prometheus metrics
│       ├── __init__.py
│       └── pipeline_metrics.py
│
├── dbt/                             # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                # Silver → structured
│   │   ├── intermediate/           # Business logic
│   │   └── marts/                  # Gold → analytics
│   ├── tests/
│   └── macros/
│
├── airflow/                         # Airflow DAGs
│   ├── dags/
│   │   ├── initial_snapshot.py
│   │   ├── reconciliation.py
│   │   └── dbt_transform.py
│   └── plugins/
│
├── monitoring/                      # Observability configs
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── provisioning/
│   │   └── dashboards/
│   └── alerting/
│       └── rules.yml
│
├── tests/                           # Test suites
│   ├── unit/
│   ├── integration/
│   └── chaos/
│
├── scripts/                         # Utility scripts
│   ├── simulate_traffic.py         # Generate realistic DB writes
│   ├── health_check.py
│   └── reconcile.py
│
├── docs/                            # Documentation
│   ├── phase-01-architecture.md    # ← You are here
│   ├── architecture-diagram.png
│   └── runbook.md
│
├── .env                             # Environment variables
├── .gitignore
├── Makefile                         # Common commands
├── requirements.txt                 # Python dependencies
└── README.md                        # Project overview
```

---

## 🔑 Key Architecture Decisions

### Decision 1: WAL-based CDC vs. Trigger-based CDC vs. Polling

| Approach | Pros | Cons | Our Choice |
|----------|------|------|-----------|
| **WAL Logical Decoding** | Zero app impact, captures all changes, ordered | Requires PG config, slightly complex | ✅ **Selected** |
| **Database Triggers** | Simple to understand | Impacts write performance, misses DDL, maintenance burden | ❌ |
| **Timestamp Polling** | Simple implementation | Misses deletes, high latency, source load | ❌ |

### Decision 2: Why Kafka (not direct DB-to-DB replication)?

- **Decoupling:** Source doesn't know about consumers. Add/remove consumers without touching source.
- **Buffering:** If target is down, events are safely stored in Kafka (retention period).
- **Replay:** Re-read events from any offset to reprocess or fix bugs.
- **Multiple Consumers:** Same event goes to MinIO, Target PG, and metrics simultaneously.
- **Ordering:** Kafka guarantees order within a partition — critical for CDC.

### Decision 3: Why MinIO Archival?

- **Compliance:** Raw event archive for auditing
- **Replay:** Rebuild entire target from archive if needed
- **Cost:** Object storage is 10-100x cheaper than database storage
- **Decoupling:** Archive independently from processing speed

### Decision 4: Exactly-Once Semantics Strategy

```
Problem: What if consumer processes an event but crashes before committing the Kafka offset?
         → On restart, it re-reads the event and processes it AGAIN (duplicate!)

Solution: Idempotent writes using event ID + deduplication table
         → Even if processed twice, the result is the same
```

### Decision 5: Soft Delete vs. Hard Delete

```
Source: DELETE FROM orders WHERE id = 5
Target Strategy: UPDATE orders SET _is_deleted = true, _deleted_at = NOW() WHERE id = 5

Why: Analytics queries need to know "what was deleted and when" for accurate historical reporting.
     Hard deletes lose information forever.
```

---

## 🌊 Failure Scenarios and How We Handle Them

| Failure | Impact | Mitigation |
|---------|--------|------------|
| Source DB crashes | No new CDC events | Kafka retains last events; resume from WAL position on restart |
| Kafka broker dies | Events temporarily undeliverable | Multi-broker cluster; replication factor = 3 |
| Consumer crashes | Events stop being processed | Auto-restart; resume from last committed offset |
| Target DB is slow | Consumer backs up | Backpressure handling; batch writes; consumer lag alerts |
| MinIO is down | Archive fails | Dead letter queue; retry logic; Airflow reconciliation |
| Network partition | Partial failures | Idempotent writes; transactional outbox pattern |
| Schema change on source | Consumer can't parse | Schema registry; backward compatibility enforcement |

---

## 📐 Non-Functional Requirements

| Requirement | Target | How We Achieve It |
|-------------|--------|-------------------|
| **Latency** | < 5 seconds source → target | Kafka streaming; no batch delays |
| **Throughput** | 1000+ events/second | Kafka partitioning; batch consumers |
| **Durability** | Zero data loss | WAL + Kafka retention + MinIO archive |
| **Ordering** | Per-entity ordering | Partition by primary key |
| **Idempotency** | No duplicates in target | Deduplication table + upsert logic |
| **Observability** | Full pipeline visibility | Prometheus metrics at every stage |
| **Recoverability** | Resume from any failure | Offset management; replay from MinIO |

---

## 🧪 Testing and Validation for This Phase

Since Phase 1 is architecture/design, validation means ensuring you UNDERSTAND the design:

### Self-Check Questions (answer these before proceeding):

1. ✅ Can you explain the data flow from INSERT to dashboard in your own words?
2. ✅ Can you explain WHY we use Kafka instead of directly writing from source to target?
3. ✅ Can you explain what happens if the consumer crashes mid-processing?
4. ✅ Can you explain the difference between Bronze, Silver, and Gold layers?
5. ✅ Can you explain why WAL-based CDC is better than polling?
6. ✅ Can you draw the architecture on paper from memory?

---

## ⚠️ Common Mistakes and Misconceptions

| Mistake | Reality |
|---------|---------|
| "CDC means copying the whole table periodically" | No — CDC captures only the CHANGES (deltas), not full snapshots |
| "Kafka is a database" | No — Kafka is a log. It stores events temporarily (retention period). It's not for queries. |
| "We need all tools from Day 1" | No — We build incrementally. Start with source + Kafka + consumer. Add layers progressively. |
| "More partitions = always better" | No — Partitions enable parallelism but add complexity. Start with partitions = number of source tables. |
| "Real-time means instant" | No — "Real-time" in data engineering typically means seconds to low minutes, not milliseconds. |
| "MinIO replaces the database" | No — MinIO is for archival (read-rarely). PostgreSQL target is for queries (read-frequently). |

---

## 🎤 Interview Questions for This Phase

### Beginner Level:
1. **Q:** What is Change Data Capture?
   **A:** CDC is a technique to identify and capture changes (inserts, updates, deletes) made to a database and deliver those changes to downstream systems in real-time or near-real-time.

2. **Q:** What is PostgreSQL WAL?
   **A:** Write-Ahead Log — PostgreSQL's durability mechanism where every transaction is written to a sequential log before being applied to data files. Logical decoding allows reading these logs as structured change events.

3. **Q:** Why use Kafka in a CDC pipeline?
   **A:** Kafka decouples producers from consumers, provides durable buffering, enables replay from any offset, supports multiple consumers reading the same events, and guarantees ordering within partitions.

### Intermediate Level:
4. **Q:** How do you ensure exactly-once processing in a CDC pipeline?
   **A:** Through idempotent writes — using a deduplication table that tracks processed event IDs. Even if an event is consumed twice (due to rebalancing or crash recovery), the write operation is idempotent (upsert with same primary key produces same result).

5. **Q:** What's the difference between logical and physical replication in PostgreSQL?
   **A:** Physical replication copies raw WAL bytes (byte-for-byte disk copy). Logical replication decodes WAL into logical change events (row-level INSERT/UPDATE/DELETE) that can be selectively consumed and transformed.

6. **Q:** How do you handle schema changes in a running CDC pipeline?
   **A:** Using a schema registry that enforces backward/forward compatibility. New fields get defaults, removed fields are deprecated gracefully. Consumers use schema evolution rules to handle multiple versions.

### Advanced Level:
7. **Q:** How would you handle a CDC pipeline that falls 2 hours behind?
   **A:** Check consumer lag metrics in Grafana. Identify bottleneck (consumer processing speed vs. sink write speed). Scale consumers horizontally if CPU-bound. Batch writes if sink-bound. Check for poison messages in DLQ. Consider temporary consumer group scale-out.

8. **Q:** How do you guarantee ordering across related tables (e.g., order before order_items)?
   **A:** Partition related events by a shared key (e.g., order_id). Within a single Kafka partition, ordering is guaranteed. Cross-table ordering requires either: (a) single topic with all related events, or (b) consumer-side buffering with watermarks.

9. **Q:** Design a CDC pipeline that handles 100K events/second with exactly-once semantics and sub-second latency.
   **A:** Multiple Kafka partitions (≥50), consumer group with one consumer per partition, async batch writes to target (batch size 1000, flush interval 500ms), Redis-based deduplication cache (faster than PG lookups), pre-allocated connection pools, and Kafka transactions for atomic offset commits + writes.

---

## ✅ Phase 1 Completion Checklist

Before moving to Phase 2, confirm you can check ALL of these:

- [ ] I understand what CDC is and why it's used in production systems
- [ ] I understand the role of PostgreSQL WAL in our pipeline
- [ ] I can explain why each technology was chosen and what it does
- [ ] I understand the data flow: Source → WAL → Kafka → Consumer → (MinIO + Target PG + Metrics)
- [ ] I understand the Bronze/Silver/Gold layering concept
- [ ] I understand what "exactly-once" means and why it matters
- [ ] I know what happens when each component fails
- [ ] I have reviewed the folder structure and understand what goes where
- [ ] I can answer at least 5 of the 9 interview questions from memory
- [ ] I can draw the architecture diagram on paper without looking

---

## 🚀 What's Next (Phase 2 Preview)

In Phase 2, we will:
- Set up Docker and Docker Compose
- Create containers for ALL services (PostgreSQL × 2, Kafka, Zookeeper, MinIO, etc.)
- Verify every service starts and is accessible
- Establish the development environment you'll use for the entire project

**Do NOT proceed to Phase 2 until you've completed the checklist above.**

Reply with "Phase 1 complete" when you're ready to continue.
