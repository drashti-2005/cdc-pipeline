# Phase 3: PostgreSQL Source Database Setup

## 🎯 Objective of This Phase

**In simple language:**
We're going to set up the "source" database — the one that our application writes to. We'll create tables that mimic a real e-commerce system (customers, products, orders). Most importantly, we'll configure PostgreSQL to broadcast its changes so our CDC pipeline can listen. Think of it like turning on a microphone in a room — PostgreSQL will now "announce" every INSERT, UPDATE, and DELETE it performs.

**In technical language:**
This phase configures PostgreSQL for logical replication by enabling WAL logical decoding, creates the OLTP schema with proper indexing, sets up a replication slot and publication, and builds a traffic simulator that generates realistic transactional workload for testing the pipeline.

---

## 📚 Concepts You Need to Understand First

### 1. What is an OLTP Database?

**Simple explanation:**
OLTP (Online Transaction Processing) is a database optimized for fast writes — handling individual operations like "customer placed an order" or "payment received." It's the database your application talks to directly. Think of it like a cash register — it records one transaction at a time, very quickly.

**Technical explanation:**
OLTP databases are optimized for high-frequency, low-latency, row-level operations (INSERT, UPDATE, DELETE). They use normalized schemas, B-tree indexes, and ACID transactions. Examples: the PostgreSQL behind your e-commerce checkout, banking transactions, or ride bookings.

### 2. What is Logical Replication in PostgreSQL?

**Simple explanation:**
PostgreSQL has two ways to copy data to another system:
- **Physical replication:** Copy the raw disk bytes (exact clone, same version required)
- **Logical replication:** Convert changes to meaningful events ("row 5 was inserted with these values") — this is what we use

Logical replication lets us selectively capture specific tables and decode changes into a format our pipeline can understand.

**Technical explanation:**
Logical replication uses the WAL logical decoding framework. PostgreSQL's WAL contains a binary stream of all changes. A logical decoding output plugin (like `pgoutput` or `wal2json`) translates this binary stream into structured change events. A replication slot tracks which WAL position a consumer has read up to, preventing PostgreSQL from deleting unread WAL segments.

### 3. What is a Replication Slot?

**Simple explanation:**
Imagine you're reading a book, and you put a bookmark in it. That bookmark tells the library "don't throw away pages before this — I haven't read them yet." A replication slot is PostgreSQL's bookmark. It tells PostgreSQL: "Keep the WAL changes from this point forward because someone hasn't consumed them yet."

**Technical explanation:**
A replication slot is a PostgreSQL mechanism that:
1. Guarantees WAL segments won't be recycled until the consumer acknowledges them
2. Tracks the consumer's read position (LSN - Log Sequence Number)
3. Persists across restarts (the slot survives database crashes)

⚠️ **Danger:** If a consumer is down for too long, the replication slot will cause WAL to accumulate, potentially filling the disk. This is why monitoring replication lag is critical.

### 4. What is a Publication?

**Simple explanation:**
A publication is like a "newsletter subscription." You tell PostgreSQL: "For these specific tables, broadcast all changes." Only tables included in the publication will emit CDC events. This lets you choose what to capture instead of capturing everything.

**Technical explanation:**
A `PUBLICATION` in PostgreSQL defines which tables participate in logical replication and which operations (INSERT, UPDATE, DELETE) are published. Combined with a replication slot and a subscriber (our CDC producer), it forms the logical replication pipeline.

### 5. What is REPLICA IDENTITY?

**Simple explanation:**
When you UPDATE or DELETE a row, how does the receiving system know WHICH row was changed? The replica identity tells PostgreSQL "include this information so the receiver can identify the row." By default, it only sends the primary key. We set it to FULL so it sends the entire old row values too.

**Technical explanation:**
`REPLICA IDENTITY` controls what information is written to WAL for UPDATE and DELETE operations:
- `DEFAULT`: Only primary key columns in the old tuple
- `FULL`: All columns in the old tuple (we use this)
- `NOTHING`: No old tuple information
- `USING INDEX`: Columns of a specific index

We use `FULL` because our consumer needs the complete "before" state for proper change tracking and soft deletes.

---

## 📁 Files We'll Create in This Phase

```
cdc-pipeline/
├── docker/
│   └── postgres-source/
│       └── init.sql              # Schema + config (runs on first container start)
├── scripts/
│   └── simulate_traffic.py      # Generates realistic INSERT/UPDATE/DELETE traffic
└── docs/
    └── phase-03-source-database.md  # This file
```

---

## 🏗️ Database Schema Design

We're modeling a simplified e-commerce system:

```
┌──────────────┐       ┌──────────────┐
│  customers   │       │   products   │
├──────────────┤       ├──────────────┤
│ id (PK)      │       │ id (PK)      │
│ email        │       │ name         │
│ name         │       │ category     │
│ created_at   │       │ price        │
│ updated_at   │       │ stock_qty    │
└──────┬───────┘       │ created_at   │
       │               │ updated_at   │
       │               └──────┬───────┘
       │                      │
       ▼                      ▼
┌──────────────────────────────────────┐
│              orders                   │
├──────────────────────────────────────┤
│ id (PK)                              │
│ customer_id (FK → customers)         │
│ status (pending/confirmed/shipped/   │
│         delivered/cancelled)          │
│ total_amount                         │
│ created_at                           │
│ updated_at                           │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│           order_items                 │
├──────────────────────────────────────┤
│ id (PK)                              │
│ order_id (FK → orders)               │
│ product_id (FK → products)           │
│ quantity                             │
│ unit_price                           │
│ created_at                           │
└──────────────────────────────────────┘
```

### Why this schema?

- **4 tables** = enough complexity to demonstrate multi-table CDC
- **Foreign keys** = demonstrates referential integrity and event ordering challenges
- **Status field** = generates UPDATEs (order lifecycle: pending → shipped → delivered)
- **stock_qty** = generates UPDATEs (decremented on purchase)
- **Timestamps** = demonstrates slowly changing dimensions

---

## 🔧 Step-by-Step Implementation

### Step 1: Create the init.sql file

This SQL script runs automatically when the PostgreSQL container starts for the first time. It:
1. Creates the schema and tables
2. Sets REPLICA IDENTITY FULL on all tables
3. Creates the publication
4. Creates the replication slot
5. Seeds initial data

### Step 2: Mount init.sql in docker-compose

We need to tell Docker to run our SQL on first startup. PostgreSQL's Docker image automatically executes any `.sql` file placed in `/docker-entrypoint-initdb.d/`.

### Step 3: Create the traffic simulator

A Python script that continuously generates realistic database operations to test our pipeline.

### Step 4: Recreate the source container

```bash
# Stop and remove the source container + its volume (clean slate)
docker compose -f docker/docker-compose.yml --env-file .env stop postgres-source
docker compose -f docker/docker-compose.yml --env-file .env rm -f postgres-source
docker volume rm cdc_postgres_source_data

# Restart (init.sql will run on fresh start)
docker compose -f docker/docker-compose.yml --env-file .env up -d postgres-source
```

### Step 5: Verify the setup

```bash
# Connect to source database
docker exec -it postgres-source psql -U cdc_user -d source_db

# Inside psql, run:
\dt                              -- List tables
SELECT * FROM customers LIMIT 5; -- Check seed data
SELECT * FROM pg_replication_slots;  -- Check replication slot
SELECT * FROM pg_publication;        -- Check publication
SHOW wal_level;                      -- Should be 'logical'
```

---

## 🧪 Testing and Validation

```bash
# 1. Verify WAL level is logical
docker exec postgres-source psql -U cdc_user -d source_db -c "SHOW wal_level;"
# Expected: logical

# 2. Verify tables exist
docker exec postgres-source psql -U cdc_user -d source_db -c "\dt"
# Expected: customers, products, orders, order_items

# 3. Verify replication slot exists
docker exec postgres-source psql -U cdc_user -d source_db -c "SELECT slot_name, plugin, slot_type FROM pg_replication_slots;"
# Expected: cdc_slot | pgoutput | logical

# 4. Verify publication exists
docker exec postgres-source psql -U cdc_user -d source_db -c "SELECT * FROM pg_publication;"
# Expected: cdc_publication

# 5. Verify publication includes all tables
docker exec postgres-source psql -U cdc_user -d source_db -c "SELECT * FROM pg_publication_tables;"
# Expected: All 4 tables listed

# 6. Verify seed data
docker exec postgres-source psql -U cdc_user -d source_db -c "SELECT count(*) FROM customers;"
# Expected: 10 (or whatever we seed)

# 7. Verify REPLICA IDENTITY
docker exec postgres-source psql -U cdc_user -d source_db -c "SELECT relname, relreplident FROM pg_class WHERE relname IN ('customers','products','orders','order_items');"
# Expected: 'f' for all (f = FULL)

# 8. Test the traffic simulator
python scripts/simulate_traffic.py --events 10 --interval 1
# Expected: Prints 10 simulated events to source DB
```

---

## ⚠️ Common Mistakes and Debugging Tips

| Problem | Cause | Fix |
|---------|-------|-----|
| `wal_level` shows `replica` not `logical` | Command args not applied | Check docker-compose.yml has `-c wal_level=logical` |
| Replication slot doesn't exist | init.sql didn't run | Remove volume and recreate container |
| "replication slot already exists" | Running init.sql twice | Drop slot first: `SELECT pg_drop_replication_slot('cdc_slot');` |
| "publication already exists" | Same as above | `DROP PUBLICATION IF EXISTS cdc_publication;` |
| Traffic simulator can't connect | Wrong port or container not ready | Ensure port 5432 is mapped and container is healthy |
| "permission denied for replication" | User lacks REPLICATION privilege | `ALTER USER cdc_user REPLICATION;` |
| init.sql has syntax error | Container starts but tables missing | Check logs: `docker logs postgres-source` |

---

## 🎤 Interview Questions for This Phase

### Beginner Level:

1. **Q:** What is the difference between physical and logical replication in PostgreSQL?
   **A:** Physical replication copies raw WAL bytes — it creates an exact binary clone that must be the same PG version. Logical replication decodes WAL into row-level change events (INSERT/UPDATE/DELETE) that can be consumed selectively by different systems regardless of version.

2. **Q:** What is a replication slot and why is it important?
   **A:** A replication slot is a bookmark in PostgreSQL's WAL that tracks what a consumer has read. It prevents PostgreSQL from deleting WAL segments that haven't been consumed yet, guaranteeing zero data loss. However, if a consumer is offline too long, WAL accumulates and can fill the disk.

3. **Q:** Why did we set `REPLICA IDENTITY FULL` on our tables?
   **A:** By default, PostgreSQL only includes primary key values in the WAL for UPDATE/DELETE operations. With FULL, it includes all column values of the old row, enabling our consumer to know the complete "before" state of a change — essential for audit trails, soft deletes, and data reconciliation.

### Intermediate Level:

4. **Q:** What happens to the replication slot if our CDC consumer goes down for 24 hours?
   **A:** The replication slot prevents WAL cleanup, so WAL files accumulate on disk. This can fill the disk and crash PostgreSQL. Mitigation: monitor `pg_replication_slots.pg_wal_lsn_diff()` for slot lag, set `max_slot_wal_keep_size` to cap WAL retention, and alert on growing lag.

5. **Q:** Why use `pgoutput` plugin instead of `wal2json`?
   **A:** `pgoutput` is PostgreSQL's built-in logical decoding output plugin — no extensions needed. It's used by native logical replication and is well-maintained. `wal2json` requires installing an extension but outputs JSON directly. We use `pgoutput` for reliability and compatibility.

6. **Q:** How would you handle an initial load (backfill) for tables that already have millions of rows?
   **A:** Take a consistent snapshot using `pg_export_snapshot()`, bulk-copy existing data with `COPY` command while recording the WAL LSN at snapshot time, then start streaming from that LSN. This guarantees no gaps between historical data and real-time changes.

### Advanced Level:

7. **Q:** How do you handle DDL changes (ALTER TABLE) in a CDC pipeline?
   **A:** DDL isn't captured by logical decoding. Strategies: (a) Use event triggers to capture DDL and publish to a separate Kafka topic, (b) Monitor `pg_catalog` for schema changes via Airflow, (c) Use a schema registry to version event schemas and handle evolution at the consumer.

8. **Q:** What is the performance impact of logical replication on the source database?
   **A:** Minimal but non-zero: (a) WAL volume increases ~10-20% due to full row images with REPLICA IDENTITY FULL, (b) Logical decoding uses CPU to parse WAL, (c) Replication slots prevent WAL cleanup. Mitigation: size WAL disk appropriately, monitor replication lag, and set `max_slot_wal_keep_size`.

---

## ✅ Phase 3 Completion Checklist

- [ ] `docker/postgres-source/init.sql` created with schema, replication slot, and publication
- [ ] Source container recreated with fresh volume (init.sql ran successfully)
- [ ] `SHOW wal_level;` returns `logical`
- [ ] All 4 tables exist with seed data
- [ ] Replication slot `cdc_slot` exists and is active
- [ ] Publication `cdc_publication` includes all 4 tables
- [ ] REPLICA IDENTITY is FULL on all tables
- [ ] Traffic simulator runs and generates operations
- [ ] I understand the difference between physical and logical replication
- [ ] I understand what a replication slot does and its risks
- [ ] I can explain the schema design choices

---

## 🚀 What's Next (Phase 4 Preview)

In Phase 4, we will:
- Create Kafka topics for each table
- Design the topic naming convention
- Configure topic partitioning strategy
- Test producing/consuming simple messages

**Reply "Phase 3 complete" when your checklist is done.**
