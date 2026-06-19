# Phase 8: Prometheus Metrics + Grafana Dashboards

## 📖 Overview

This phase adds **observability** to the CDC pipeline. Without monitoring, you're flying blind - you won't know when things break until users complain.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MONITORING ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐         ┌──────────────┐         ┌──────────────┐    │
│  │  Producer    │         │  Consumer    │         │   Grafana    │    │
│  │  (metrics)   │────────►│  /metrics    │────────►│  Dashboards  │    │
│  └──────────────┘         └──────────────┘         └──────────────┘    │
│         │                        │                        ▲            │
│         │                        │                        │            │
│         └────────────┬───────────┘                        │            │
│                      ▼                                    │            │
│              ┌──────────────┐                             │            │
│              │  Prometheus  │─────────────────────────────┘            │
│              │  (scrapes)   │                                          │
│              └──────────────┘                                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Simple Explanation

**What is Monitoring?**
Think of it like a car dashboard:
- **Speedometer** → Events processed per second (throughput)
- **Fuel gauge** → Buffer fill levels, cache usage
- **Warning lights** → Alerts for errors and failures
- **Trip computer** → Processing latency percentiles

Without a dashboard, you'd have no idea how fast you're going or when you're about to run out of gas!

**Prometheus** = The data collector that reads your "sensors"
**Grafana** = The dashboard that displays the data visually
**Alerts** = The warning lights that beep when something's wrong

---

## 🔧 Technical Explanation

### Prometheus Metrics Types

| Type | Purpose | Example |
|------|---------|---------|
| **Counter** | Cumulative values that only go up | `events_processed_total` |
| **Gauge** | Current values that go up and down | `buffer_size`, `cache_entries` |
| **Histogram** | Distribution of values (percentiles) | `processing_duration_seconds` |
| **Summary** | Similar to histogram, pre-calculated | Not commonly used |

### Key Metrics We Track

```python
# Throughput (how fast)
cdc_consumer_events_total        # Events consumed from Kafka
cdc_consumer_events_processed    # Events written to sinks

# Latency (how slow)
cdc_consumer_processing_duration_seconds  # Time per event
cdc_postgres_write_duration_seconds       # Time per DB write

# Errors (how broken)
cdc_consumer_events_failed_total  # Events that failed
cdc_postgres_connection_errors    # DB connection issues

# Resources (how full)
cdc_minio_buffer_size            # Events waiting for flush
cdc_dedup_cache_size             # Deduplication cache entries
```

### Pull vs Push Model

**Prometheus uses PULL** (it scrapes our /metrics endpoint):
```
Every 10 seconds:
  Prometheus → GET http://consumer:8000/metrics
  Consumer → Returns metric values
```

**Why pull?**
- Prometheus controls the pace (no overload)
- If service is down, Prometheus knows immediately
- No network config needed in services

---

## 📁 Files Created/Modified

### New Files
- `src/metrics/pipeline_metrics.py` - All metric definitions
- `monitoring/alerting/cdc-alerts.yml` - Alert rules

### Modified Files
- `src/consumer/kafka_consumer.py` - Metrics server + recording
- `src/consumer/event_router.py` - Success/failure metrics
- `src/consumer/postgres_sink.py` - Write latency metrics
- `src/consumer/minio_sink.py` - Buffer + write metrics
- `src/consumer/deduplication.py` - Cache size metrics
- `monitoring/grafana/dashboards/cdc-pipeline-overview.json` - Full dashboard
- `monitoring/prometheus/prometheus.yml` - Alert rules config

---

## 🧪 Testing

### 1. Start Infrastructure
```bash
cd docker
docker-compose up -d prometheus grafana
```

### 2. Run Consumer (Metrics will be exposed)
```bash
cd cdc-pipeline
python -m src.consumer.kafka_consumer
```

### 3. Check Metrics Endpoint
```bash
# In a new terminal
curl http://localhost:8000/metrics
```

You should see output like:
```
# HELP cdc_consumer_events_total Total number of CDC events consumed
# TYPE cdc_consumer_events_total counter
cdc_consumer_events_total{topic="cdc.source.public.customers"} 42.0

# HELP cdc_consumer_processing_duration_seconds Time to process event
# TYPE cdc_consumer_processing_duration_seconds histogram
cdc_consumer_processing_duration_seconds_bucket{le="0.001"} 38.0
cdc_consumer_processing_duration_seconds_bucket{le="0.005"} 40.0
```

### 4. Check Prometheus
- Open http://localhost:9090
- Go to Status → Targets
- Verify "cdc-pipeline" target is UP (green)

### 5. View Grafana Dashboard
- Open http://localhost:3000
- Login: admin / admin (or your configured password)
- Import dashboard from `monitoring/grafana/dashboards/cdc-pipeline-overview.json`
- Or navigate to Dashboards → CDC Pipeline Overview

---

## 📊 Dashboard Panels

| Panel | What It Shows | Alert Threshold |
|-------|--------------|-----------------|
| Events/sec (Consumed) | Rate of Kafka consumption | - |
| Events/sec (Processed) | Rate of successful processing | - |
| Failures/sec | Rate of failed events | > 0.05 (5%) |
| Dedup Cache Size | Number of cached event IDs | > 90% capacity |
| Processing Latency | p50, p95, p99 latencies | p99 > 1s |
| Sink Write Latency | PostgreSQL and MinIO write times | p95 > 0.5s |
| Failed Events | Stacked chart by sink/error | Any |
| MinIO Buffer | Events waiting for flush | > 1000 |

---

## 🚨 Alert Rules

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| CDCConsumerDown | Metrics endpoint unreachable | CRITICAL | Page on-call |
| CDCHighFailureRate | >5% events failing | WARNING | Check DLQ |
| CDCNoEventsProcessed | 0 events in 10 min | WARNING | Check Kafka |
| CDCHighProcessingLatency | p99 > 1 second | WARNING | Check sinks |
| CDCPostgresConnectionErrors | >5 errors in 5 min | WARNING | Check DB |
| CDCMinIOBufferBacklog | >1000 events buffered | WARNING | Check MinIO |

---

## 💡 Interview Questions

### Q: Why use Prometheus over other monitoring systems?
**A:** Prometheus is:
1. **Pull-based** - No agents needed, just HTTP endpoints
2. **Multi-dimensional** - Labels allow slicing data (by table, operation)
3. **Powerful query language** - PromQL for complex aggregations
4. **Built for containers** - Native Kubernetes integration
5. **Reliable** - Local storage survives network issues

### Q: What metrics would you add for a production system?
**A:** Additional metrics I'd track:
1. **Kafka consumer lag** - How far behind real-time
2. **WAL position delta** - Source vs consumer LSN difference
3. **Resource usage** - Memory, CPU, connections
4. **Business metrics** - Orders/minute, revenue processed
5. **Schema changes** - Track DDL events

### Q: How do you handle high-cardinality labels?
**A:** High cardinality (many unique values) kills Prometheus:
- BAD: `user_id`, `event_id`, `timestamp` as labels
- GOOD: `table`, `operation`, `status` as labels
- Solution: Use logs for high-cardinality data, metrics for aggregates

### Q: What's the difference between metrics, logs, and traces?
**A:**
| Type | Purpose | Cardinality | Example |
|------|---------|-------------|---------|
| **Metrics** | Aggregate numbers | Low | "100 events/sec" |
| **Logs** | Event details | High | "User 123 created order" |
| **Traces** | Request flow | Medium | "Order took 200ms across 5 services" |

Use all three together for full observability!

### Q: How would you set alert thresholds?
**A:** Start with SLOs (Service Level Objectives):
1. Define acceptable latency (e.g., 99% under 500ms)
2. Define acceptable error rate (e.g., 0.1%)
3. Set alerts at 50% of budget consumed
4. Tune based on historical data
5. Avoid alert fatigue - fewer, actionable alerts

---

## 🔜 Next Steps (Phase 9)

Phase 9 will add **End-to-End Testing** with:
- Integration tests that verify full pipeline flow
- Test fixtures for source database
- Kafka assertion helpers
- MinIO and PostgreSQL verification

---

## 📚 References

- [Prometheus Docs](https://prometheus.io/docs/introduction/overview/)
- [Prometheus Client Python](https://github.com/prometheus/client_python)
- [Grafana Dashboard Guide](https://grafana.com/docs/grafana/latest/dashboards/)
- [Four Golden Signals](https://sre.google/sre-book/monitoring-distributed-systems/)
