# CDC Pipeline - Change Data Capture for Database Replication & Analytics

A production-grade CDC pipeline that captures INSERT, UPDATE, and DELETE events from PostgreSQL using WAL logical decoding, streams them through Kafka, archives to MinIO, replicates to a target PostgreSQL, transforms with dbt, and monitors with Prometheus + Grafana.

## Architecture Overview

```
PostgreSQL (Source) → WAL → Kafka → Python Consumer → MinIO (Archive)
                                                    → PostgreSQL (Target)
                                                    → Prometheus (Metrics)
                                          ↓
                            Airflow → dbt → Superset (BI)
                            Grafana (Monitoring)
```

## Tech Stack

| Technology | Role |
|-----------|------|
| PostgreSQL | Source & Target databases |
| Apache Kafka | Event streaming platform |
| Python | CDC producer & consumer |
| MinIO | S3-compatible object storage (Bronze layer) |
| dbt | Data transformations (Silver → Gold) |
| Apache Airflow | Workflow orchestration |
| Apache Superset | Business intelligence |
| Prometheus | Metrics collection |
| Grafana | Monitoring dashboards |
| Docker | Containerization |

## Project Phases

- [x] Phase 1: Architecture & System Design
- [ ] Phase 2: Docker Environment Setup
- [ ] Phase 3: PostgreSQL Source Database Setup
- [ ] Phase 4: Kafka Cluster Setup
- [ ] Phase 5: CDC Capture from PostgreSQL WAL
- [ ] Phase 6: Kafka Topic Design and Event Schema
- [ ] Phase 7: Python Consumer Development
- [ ] Phase 8: MinIO Archival Layer
- [ ] Phase 9: Target PostgreSQL Replication Layer
- [ ] Phase 10: Exactly-Once Processing
- [ ] Phase 11: Airflow Orchestration
- [ ] Phase 12: dbt Transformations
- [ ] Phase 13: Data Quality & Reconciliation
- [ ] Phase 14: Superset Dashboards
- [ ] Phase 15: Prometheus Metrics
- [ ] Phase 16: Grafana Monitoring
- [ ] Phase 17: Failure Recovery & Replay
- [ ] Phase 18: Chaos Testing
- [ ] Phase 19: Production Hardening
- [ ] Phase 20: Documentation & Interview Prep

## Getting Started

Start with `docs/phase-01-architecture.md` and proceed phase by phase.
