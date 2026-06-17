# Phase 5: CDC Capture from PostgreSQL WAL

## 🎯 Objective of This Phase

**In simple language:**
This is the HEART of the entire project. In this phase, we write the code that watches PostgreSQL's change journal (WAL) and converts every database change into a Kafka message. Think of it like hiring a secretary who sits next to your database, watches every transaction as it happens, and immediately sends a summary message to the right mailbox (Kafka topic).

**In technical language:**
This phase implements the CDC producer — a Python process that connects to the PostgreSQL source database via a logical replication connection, reads change events from the `cdc_slot` replication slot using the `pgoutput` logical decoding plugin, parses the binary protocol messages into structured `CDCEvent` objects, and publishes them to the appropriate Kafka topics with transactional guarantees.

---

## 📚 Concepts You Need to Understand First

### 1. What is PostgreSQL Logical Decoding?

**Simple explanation:**
PostgreSQL records every change in a special journal (WAL). Normally this is in binary format only PostgreSQL understands. "Logical decoding" is like hiring a translator — it converts the binary journal entries into human-readable messages ("row 5 in orders table was updated from status=pending to status=confirmed").

**Technical explanation:**
Logical decoding is a PostgreSQL API that translates WAL binary records into a stream of row-level logical change events. It uses an output plugin (`pgoutput` in our case) to format these events. We connect using the replication protocol (not a regular SQL connection) and stream changes in real-time. The `pg_logical_replication_slot` we created in Phase 3 tracks our read position.

### 2. What is `pgoutput`?

**Simple explanation:**
`pgoutput` is PostgreSQL's built-in translator for logical decoding. It comes installed with PostgreSQL — no extensions needed. It formats WAL entries into a well-defined binary protocol that includes the table name, operation type, and before/after row values.

**Technical explanation:**
`pgoutput` is the standard logical replication output plugin introduced in PostgreSQL 10. It implements the Logical Replication Protocol (LRP) — a binary streaming protocol that encodes change events. The protocol includes message types: `B` (begin), `C` (commit), `I` (insert), `U` (update), `D` (delete), `R` (relation/schema). Our Python code decodes these message types.

### 3. What is a Replication Connection?

**Simple explanation:**
A regular PostgreSQL connection is for SQL queries (SELECT, INSERT, etc.). A replication connection is a special type for streaming changes. It speaks a different protocol — instead of queries, it streams a continuous feed of "this changed, that changed." We use `psycopg2` with special parameters to open this type of connection.

**Technical explanation:**
Replication connections are established with `replication=database` parameter. They use the `START_REPLICATION` command instead of SQL. The connection streams `XLogData` messages (WAL segments) continuously. The consumer sends feedback (`Standby Status Update`) to acknowledge processed LSN positions, allowing the replication slot to advance.

### 4. What is an LSN (Log Sequence Number)?

**Simple explanation:**
Every change in PostgreSQL's WAL is assigned a unique address called LSN. It's like a page number in a book — "I've read up to page 1542, continue from page 1543." We send this position back to PostgreSQL so it knows we've processed changes and can clean up old WAL files.

**Technical explanation:**
LSN (Log Sequence Number) is a 64-bit integer representing a byte position in the WAL stream, formatted as `X/YYYYYYYY`. It is strictly monotonically increasing. The replication slot tracks the `confirmed_flush_lsn` — the position up to which we've committed processing. WAL segments before this LSN can be recycled by PostgreSQL's WAL cleanup process.

### 5. What is the pgoutput Message Protocol?

**Simple explanation:**
The `pgoutput` plugin sends messages like: "BEGIN transaction 1234 → Table 'orders' was updated: row before = {id:1, status:pending}, row after = {id:1, status:confirmed} → COMMIT transaction 1234." We parse each message type.

**Technical explanation:**
The pgoutput protocol sends these message types (each starts with a 1-byte type indicator):
- `B` = Begin (transaction XID, LSN, commit time)
- `R` = Relation (table OID, schema, table name, column definitions)
- `I` = Insert (relation OID, new tuple)
- `U` = Update (relation OID, old tuple, new tuple)
- `D` = Delete (relation OID, old tuple)
- `C` = Commit (LSN confirmation)

Our decoder maintains a `relation_map` (OID → table schema) populated from `R` messages, then uses it to decode `I/U/D` messages into named column values.

### 6. What is Transactional Publishing?

**Simple explanation:**
Imagine you're sending 5 related emails. You don't want 3 sent and 2 lost if your internet cuts out. You want either ALL 5 sent or NONE. In Kafka, we batch all changes from one database transaction and flush them together, then advance our WAL position only after Kafka confirms delivery.

**Technical explanation:**
We collect all CDC events within a single PostgreSQL transaction (between `B` and `C` messages) in memory. Only after all events are successfully produced to Kafka do we:
1. Commit the Kafka producer (ensure durability)
2. Send `Standby Status Update` to PostgreSQL with the new LSN
This prevents data loss: if the process crashes between receiving and producing, we re-read from the last confirmed LSN on restart.

---

## 📁 Files Created in This Phase

```
cdc-pipeline/
├── src/
│   └── producer/
│       ├── config.py          # Producer configuration (env vars, connection params)
│       ├── wal_reader.py      # Reads PostgreSQL WAL via logical replication
│       └── kafka_producer.py  # Publishes CDC events to Kafka topics
└── docs/
    └── phase-05-cdc-capture.md    # This file
```

---

## 🏗️ Producer Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    CDC Producer Process                   │
│                                                          │
│  ┌─────────────┐    ┌──────────────┐   ┌─────────────┐ │
│  │  WAL Reader  │───▶│ Event Parser │──▶│   Kafka     │ │
│  │             │    │              │   │  Publisher  │ │
│  │ Replication │    │ B → buffer   │   │             │ │
│  │ Connection  │    │ R → schema   │   │ key=PK      │ │
│  │             │    │ I/U/D → evt  │   │ value=JSON  │ │
│  │ polls WAL   │    │ C → flush    │   │             │ │
│  └──────┬──────┘    └──────────────┘   └──────┬──────┘ │
│         │                                       │        │
│         │◀──── LSN Feedback (on flush) ─────────┘        │
└─────────┼───────────────────────────────────────────────┘
          │ Replication Protocol
          ▼
    PostgreSQL (cdc_slot)
```

### Processing Flow:

```
1. Connect to PostgreSQL via replication protocol
2. START_REPLICATION from cdc_slot at current LSN
3. Loop:
   a. Poll for next message
   b. On B (Begin):  start buffering events for this transaction
   c. On R (Relation): update our schema cache
   d. On I (Insert):  build CDCEvent(op=INSERT, after=new_row)
   e. On U (Update):  build CDCEvent(op=UPDATE, before=old_row, after=new_row)
   f. On D (Delete):  build CDCEvent(op=DELETE, before=old_row)
   g. On C (Commit):  publish all buffered events to Kafka → advance LSN
4. On error: log + retry from last confirmed LSN
```

---

## 🧪 Testing and Validation

```bash
# 1. Start the CDC producer
python -m src.producer.kafka_producer

# 2. In another terminal, run the traffic simulator
python scripts/simulate_traffic.py --events 20 --interval 0.5

# 3. Verify events appear in Kafka
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.source.public.orders \
  --from-beginning \
  --max-messages 5

# 4. Expected output: JSON CDC events like:
# {"event_id":"...","operation":"INSERT","source":{"table":"orders"},"after":{...}}
# {"event_id":"...","operation":"UPDATE","source":{"table":"orders"},"before":{...},"after":{...}}
```

---

## ⚠️ Common Mistakes and Debugging Tips

| Problem | Cause | Fix |
|---------|-------|-----|
| "replication slot does not exist" | Slot was dropped or DB recreated | Re-run Phase 3 init.sql |
| "publication does not exist" | Publication missing | `CREATE PUBLICATION cdc_publication FOR ALL TABLES;` |
| No events from existing rows | Replication captures only NEW changes | Run traffic simulator to generate new changes |
| "could not connect: FATAL: role not allowed to login" | User missing REPLICATION privilege | `ALTER USER cdc_user REPLICATION;` |
| LSN not advancing | Producer not sending feedback | Check `send_feedback()` is called on commit |
| Duplicate events on restart | Normal — we re-read since last confirmed LSN | Deduplication handles this in Phase 10 |
| WAL growing unboundedly | Producer offline, slot not advancing | Monitor slot lag; set `max_slot_wal_keep_size` |

---

## 🎤 Interview Questions for This Phase

### Beginner Level:

1. **Q:** How does the CDC producer connect to PostgreSQL differently from a normal application?
   **A:** It uses a replication connection (`replication=database` parameter) which speaks the Replication Protocol instead of SQL. It issues `START_REPLICATION` to begin streaming WAL changes from a specific slot and LSN position.

2. **Q:** What does the CDC producer do with the LSN (Log Sequence Number)?
   **A:** It tracks the LSN of the last successfully published Kafka batch and sends it back to PostgreSQL as a `Standby Status Update`. This tells PostgreSQL "I've processed everything up to this point, you can clean up older WAL segments." If the producer restarts, it resumes from the last confirmed LSN.

3. **Q:** What happens if a database transaction modifies 10 rows? How many Kafka messages are produced?
   **A:** 10 Kafka messages — one per row change. However, they're buffered in memory until the transaction COMMITs (when `C` message arrives), then all 10 are produced to Kafka atomically. This preserves transaction boundaries.

### Intermediate Level:

4. **Q:** How does the producer know which columns a row has? PostgreSQL's WAL doesn't include column names in every message.
   **A:** The `pgoutput` protocol sends `R` (Relation) messages whenever a new table appears in the stream. These contain the table OID, column names, and types. Our producer maintains a `relation_map` cache (OID → schema definition) and looks up column names when decoding `I/U/D` messages.

5. **Q:** Why do we buffer events per transaction instead of publishing immediately on each row change?
   **A:** To preserve atomicity. A single PostgreSQL transaction that modifies 5 rows should either produce all 5 Kafka messages or none. Publishing immediately would risk partial delivery if the process crashes between row 3 and row 4 of the transaction.

6. **Q:** What happens to the pipeline if the source PostgreSQL database restarts?
   **A:** The replication slot persists across PostgreSQL restarts. When PostgreSQL comes back up, the WAL changes that occurred before the crash are still in WAL (the slot prevents cleanup). Our producer reconnects and resumes streaming from the last confirmed LSN — no data is lost.

### Advanced Level:

7. **Q:** How do you handle DDL changes (ALTER TABLE ADD COLUMN) in the running producer?
   **A:** The `pgoutput` plugin sends a new `R` (Relation) message when the table schema changes. Our producer detects this by comparing the new relation message with the cached schema. If they differ, we update the cache, log a schema change event, and handle backward compatibility for in-flight messages.

8. **Q:** If Kafka is down and the producer can't publish, what happens?
   **A:** The producer should pause and retry (with backoff). We do NOT advance the LSN — this means the replication slot holds its position and WAL accumulates. Once Kafka recovers, we resume publishing and advance the slot. The tradeoff is disk usage on the source DB. This is why monitoring slot lag is critical.

---

## ✅ Phase 5 Completion Checklist

- [ ] `src/producer/config.py` created with all configuration
- [ ] `src/producer/wal_reader.py` created and reads pgoutput messages
- [ ] `src/producer/kafka_producer.py` runs and publishes CDC events
- [ ] Producer connects to replication slot successfully
- [ ] INSERT events visible in Kafka after running traffic simulator
- [ ] UPDATE events visible in Kafka (with `before` and `after` populated)
- [ ] DELETE events visible in Kafka (with `before` populated, `after` null)
- [ ] LSN advances after each committed transaction
- [ ] Producer recovers correctly after stopping and restarting
- [ ] I understand the pgoutput protocol message types
- [ ] I understand why we buffer per transaction

---

## 🚀 What's Next (Phase 6 Preview)

In Phase 6, we will:
- Build the Python consumer that reads from Kafka
- Route events to MinIO (archive) and Target PostgreSQL (replication)
- Implement basic error handling and the dead letter queue

**Reply "Phase 5 complete" when your checklist is done.**
