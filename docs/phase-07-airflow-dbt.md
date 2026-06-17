# Phase 7: Airflow Orchestration + dbt Transformations

## What We Built

This phase adds the **transformation layer** to complete the medallion architecture:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      MEDALLION ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────┐       ┌─────────────┐       ┌─────────────┐           │
│  │   BRONZE    │       │   SILVER    │       │    GOLD     │           │
│  │   (MinIO)   │──────►│  (Staging)  │──────►│   (Marts)   │           │
│  │             │       │             │       │             │           │
│  │ Raw CDC     │       │ Cleaned     │       │ Aggregated  │           │
│  │ Events      │       │ Normalized  │       │ Metrics     │           │
│  │ (JSON)      │       │ (Views)     │       │ (Tables)    │           │
│  └─────────────┘       └─────────────┘       └─────────────┘           │
│         │                                           │                   │
│         │              AIRFLOW                      │                   │
│         │         ┌───────────────┐                 │                   │
│         └────────►│   Schedule    │◄────────────────┘                   │
│                   │   Orchestrate │                                     │
│                   │   Monitor     │                                     │
│                   └───────────────┘                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Simple Explanation

**Think of it like a factory assembly line:**
- **Bronze (Warehouse):** Raw materials arrive (CDC events)
- **Silver (Processing):** Clean, sort, standardize the materials
- **Gold (Finished Goods):** Create products ready for customers (business reports)
- **Airflow (Factory Manager):** Schedules when each step runs

---

## Files Created

| Path | Description |
|------|-------------|
| [dbt/dbt_project.yml](../dbt/dbt_project.yml) | dbt project configuration |
| [dbt/profiles.yml](../dbt/profiles.yml) | Database connection settings |
| [dbt/models/staging/sources.yml](../dbt/models/staging/sources.yml) | Source table definitions |
| [dbt/models/staging/stg_customers.sql](../dbt/models/staging/stg_customers.sql) | Customer cleaning |
| [dbt/models/staging/stg_products.sql](../dbt/models/staging/stg_products.sql) | Product cleaning |
| [dbt/models/staging/stg_orders.sql](../dbt/models/staging/stg_orders.sql) | Order cleaning |
| [dbt/models/staging/stg_order_items.sql](../dbt/models/staging/stg_order_items.sql) | Line items |
| [dbt/models/marts/mart_daily_revenue.sql](../dbt/models/marts/mart_daily_revenue.sql) | Revenue metrics |
| [dbt/models/marts/mart_customer_360.sql](../dbt/models/marts/mart_customer_360.sql) | Customer profiles |
| [dbt/models/marts/mart_product_performance.sql](../dbt/models/marts/mart_product_performance.sql) | Product sales |
| [airflow/dags/cdc_dbt_dag.py](../airflow/dags/cdc_dbt_dag.py) | dbt orchestration DAG |
| [airflow/dags/cdc_quality_dag.py](../airflow/dags/cdc_quality_dag.py) | Data quality DAG |

---

## dbt Models Overview

### Silver Layer (Staging)

| Model | Source | Transformations |
|-------|--------|-----------------|
| `stg_customers` | customers | Normalize email, create full_name, account_age |
| `stg_products` | products | Add price_tier, stock_status flags |
| `stg_orders` | orders | Add status_category, time dimensions |
| `stg_order_items` | order_items | Calculate line_total |

### Gold Layer (Marts)

| Model | Purpose | Key Metrics |
|-------|---------|-------------|
| `mart_daily_revenue` | Finance dashboards | gross_revenue, completion_rate, mtd_revenue |
| `mart_customer_360` | CRM/Marketing | lifetime_revenue, customer_tier, engagement_status |
| `mart_product_performance` | Product/Inventory | sales_velocity, inventory_alert, revenue_rank |

---

## How to Test

### 1. Start Airflow
```bash
docker compose -f docker/docker-compose.yml --env-file .env up -d airflow airflow-db
```

### 2. Wait for Airflow to initialize (takes ~60 seconds)
```bash
docker logs -f airflow 2>&1 | head -50
```

### 3. Access Airflow UI
Open http://localhost:8080
- Username: `admin`
- Password: `admin`

### 4. Test dbt locally (without Airflow)
```bash
# Install dbt
pip install dbt-postgres

# Set environment variables
export TARGET_PG_HOST=127.0.0.1
export TARGET_PG_PORT=5435
export TARGET_PG_DB=target_db
export TARGET_PG_USER=target_user
export TARGET_PG_PASSWORD=target_password

# Run dbt
cd dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 5. Verify Silver layer created
```bash
docker exec postgres-target sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c '\dt silver.*'"
```

### 6. Verify Gold layer created
```bash
docker exec postgres-target sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c '\dt gold.*'"
```

### 7. Query the Gold tables
```bash
# Daily revenue
docker exec postgres-target sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c 'SELECT * FROM gold.mart_daily_revenue LIMIT 5;'"

# Customer 360
docker exec postgres-target sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c 'SELECT customer_id, full_name, customer_tier, lifetime_revenue FROM gold.mart_customer_360 LIMIT 5;'"

# Product performance
docker exec postgres-target sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c 'SELECT product_name, total_revenue, sales_velocity, inventory_alert FROM gold.mart_product_performance LIMIT 5;'"
```

---

## Interview Questions

### Q1: What is the medallion architecture?
**Answer:** A data lake pattern with three layers:
- **Bronze:** Raw, immutable data (our MinIO archives)
- **Silver:** Cleaned, normalized, deduplicated data (staging models)
- **Gold:** Business-level aggregations (mart models)

Benefits: Clear separation of concerns, data quality improves at each layer, easy to debug issues by checking upstream layers.

### Q2: Why use dbt instead of raw SQL scripts?
**Answer:**
1. **Modularity:** Models reference each other via `{{ ref() }}`
2. **Testing:** Built-in data quality tests
3. **Documentation:** Auto-generated lineage graphs
4. **Version control:** SQL files are just code
5. **Incremental processing:** Only process new data

### Q3: Why materialize staging as views and marts as tables?
**Answer:**
- **Views (Silver):** Always reflect current source data, no storage cost
- **Tables (Gold):** Pre-computed for fast queries, business users need speed

Trade-off: Tables use storage but are faster; views are always fresh but slower.

### Q4: What does Airflow add to the pipeline?
**Answer:**
1. **Scheduling:** Run dbt hourly/daily automatically
2. **Dependencies:** Ensure Silver runs before Gold
3. **Monitoring:** UI shows run history, failures
4. **Alerting:** Notify on failures
5. **Backfills:** Reprocess historical data

### Q5: How would you handle dbt model failures?
**Answer:**
1. Airflow retries automatically (configured retries)
2. Check logs in Airflow UI
3. Use `dbt run --select failed` to rerun only failed models
4. Add tests to prevent bad data
5. Set up Slack/email alerts for persistent failures

### Q6: What's the difference between `ref()` and `source()`?
**Answer:**
- `{{ source('cdc', 'customers') }}` - Points to raw source tables (Bronze)
- `{{ ref('stg_customers') }}` - Points to another dbt model

dbt uses these to build the dependency graph and run models in correct order.

---

## Key Concepts Summary

| Concept | What It Means |
|---------|---------------|
| Bronze | Raw, immutable data |
| Silver | Cleaned, validated data |
| Gold | Business-ready aggregations |
| Staging | Transform raw → clean |
| Marts | Aggregate for business use |
| Materialization | View (live) vs Table (snapshot) |
| DAG | Directed Acyclic Graph (workflow) |
| Freshness | How recent is the data? |

---

## Next Phase

**Phase 8: Prometheus Monitoring + Grafana Dashboards**
- Expose metrics from producer/consumer
- Create Grafana dashboards for CDC throughput
- Set up alerts for pipeline issues
