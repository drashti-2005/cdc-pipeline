# CDC Pipeline - Quick Start Guide

Get your CDC pipeline running in 5 minutes! 🚀

## Prerequisites

- Docker Desktop installed and running
- Git installed
- Python 3.10+ installed (for local development)

## Step 1: Clone and Setup (1 min)

```bash
# Clone the repository
git clone https://github.com/drashti-2005/cdc-pipeline.git
cd cdc-pipeline

# Copy environment file
cp .env.example .env

# (Optional) Edit .env to customize settings
# Default values work out of the box!
```

## Step 2: Start Services (2 min)

```bash
# Start all infrastructure services
make up

# Or manually:
docker compose -f docker/docker-compose.yml --env-file .env up -d
```

Wait for services to be healthy:
```bash
make status
```

## Step 3: Verify Everything Works (1 min)

### Check Postgres Source Database
```bash
make psql-source
# In psql: \dt
# You should see: customers, orders, products tables
# Exit: \q
```

### Check Kafka Topics
```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Check MinIO (S3 Storage)
Open browser: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

### Check Prometheus Metrics
Open browser: http://localhost:9090

### Check Grafana Dashboards
Open browser: http://localhost:3000
- Username: `admin`
- Password: `admin`

## Step 4: Run the Pipeline (1 min)

### Simulate Traffic (Generate Events)
```bash
make simulate
```

This inserts/updates/deletes data in source DB, which triggers CDC events!

### Watch CDC Events Flow
```bash
# Watch Kafka consumer logs
make logs-consumer

# Watch source database changes
make logs-source
```

### Check Data Reconciliation
```bash
make reconcile
```

## Step 5: Explore Features

### Run Data Quality Checks
```bash
python -c "from quality import QualityChecker; print('Quality framework loaded ✓')"
```

### View Pipeline Metrics
```bash
make health
```

### Run Tests
```bash
# Unit tests
make test-unit

# Integration tests
make test-integration

# All tests
make test
```

### Run dbt Transformations
```bash
make dbt-run
```

## Common Commands

| Command | Description |
|---------|-------------|
| `make up` | Start all services |
| `make down` | Stop all services |
| `make logs` | View all logs |
| `make status` | Check service health |
| `make simulate` | Generate test traffic |
| `make health` | Run health checks |
| `make reconcile` | Verify data consistency |
| `make clean` | Remove containers & volumes |
| `make test` | Run all tests |

## Troubleshooting

### Services won't start?
```bash
# Clean everything and restart
make clean
make up
```

### Can't connect to Postgres?
```bash
# Check if container is running
docker ps | grep postgres

# Check logs
make logs-source
make logs-target
```

### Kafka not working?
```bash
# Check Kafka logs
make logs-kafka

# List topics
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Need to reset everything?
```bash
# Nuclear option - removes EVERYTHING
make clean-all
```

## Architecture Overview

```
PostgreSQL Source → WAL Reader → Kafka → Consumer → MinIO Archive
                                                  → PostgreSQL Target
                                                  → Metrics (Prometheus)
                                  ↓
                    Airflow → dbt → Superset
                    Grafana (Monitoring)
```

## What's Next?

1. **Production Setup**: See `docs/phase-19-cicd.md` for production hardening
2. **Custom Logic**: Modify `src/consumer/event_processor.py` for your business logic
3. **Monitoring**: Customize Grafana dashboards in `monitoring/grafana/dashboards/`
4. **Data Quality**: Add custom rules in `src/quality/data_quality.py`
5. **Schema Evolution**: See `docs/phase-15-schema-evolution.md`

## Need Help?

- 📚 Full documentation: `docs/` folder
- 🐛 Issues: GitHub Issues
- 💬 Questions: GitHub Discussions

---

**Enjoy your CDC pipeline!** 🎉
