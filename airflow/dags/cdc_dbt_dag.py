"""
CDC Pipeline Orchestration DAG
===============================
Schedules dbt transformations to keep Silver/Gold layers fresh.

WHAT THIS DAG DOES:
1. Runs dbt to transform Bronze → Silver → Gold
2. Tests data quality
3. Alerts on failures

SCHEDULE:
- Runs every hour to keep analytics fresh
- CDC consumer runs continuously (separate process)
- dbt processes the accumulated changes

INTERVIEW TIP:
This is a "batch over streaming" pattern. The CDC pipeline is real-time,
but dbt transformations run in batches for efficiency.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ============================================================
# DAG Default Arguments
# ============================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="cdc_dbt_transformations",
    default_args=default_args,
    description="Transform CDC data: Bronze → Silver → Gold using dbt",
    schedule_interval="@hourly",  # Run every hour
    start_date=days_ago(1),
    catchup=False,  # Don't backfill missed runs
    tags=["cdc", "dbt", "transformations"],
) as dag:

    # ========================================================
    # Task 1: dbt debug - verify connection
    # ========================================================
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="cd /opt/dbt && dbt debug --profiles-dir /opt/dbt",
        doc_md="""
        ### dbt Debug
        Verifies dbt can connect to the target PostgreSQL database.
        If this fails, check database connectivity.
        """,
    )

    # ========================================================
    # Task 2: dbt run staging models (Silver layer)
    # ========================================================
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/dbt && dbt run --select staging --profiles-dir /opt/dbt",
        doc_md="""
        ### Silver Layer (Staging)
        Transforms raw CDC data into cleaned, normalized views.
        Models: stg_customers, stg_products, stg_orders, stg_order_items
        """,
    )

    # ========================================================
    # Task 3: dbt test staging models
    # ========================================================
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command="cd /opt/dbt && dbt test --select staging --profiles-dir /opt/dbt",
        doc_md="""
        ### Test Silver Layer
        Runs data quality tests on staging models:
        - Uniqueness tests
        - Not null tests
        - Accepted values tests
        """,
    )

    # ========================================================
    # Task 4: dbt run mart models (Gold layer)
    # ========================================================
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/dbt && dbt run --select marts --profiles-dir /opt/dbt",
        doc_md="""
        ### Gold Layer (Marts)
        Creates business-ready aggregated tables.
        Models: mart_daily_revenue, mart_customer_360, mart_product_performance
        """,
    )

    # ========================================================
    # Task 5: dbt test mart models
    # ========================================================
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="cd /opt/dbt && dbt test --select marts --profiles-dir /opt/dbt",
        doc_md="""
        ### Test Gold Layer
        Validates mart models have correct data:
        - Primary keys are unique
        - Status values are valid
        - No null critical fields
        """,
    )

    # ========================================================
    # Task 6: Generate dbt docs
    # ========================================================
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/dbt && dbt docs generate --profiles-dir /opt/dbt",
        doc_md="""
        ### Generate Documentation
        Creates the dbt documentation site with lineage graphs.
        Access via: dbt docs serve
        """,
    )

    # ========================================================
    # Task Dependencies (DAG Flow)
    # ========================================================
    # 
    #   dbt_debug
    #       │
    #       ▼
    #   dbt_run_staging
    #       │
    #       ▼
    #   dbt_test_staging
    #       │
    #       ▼
    #   dbt_run_marts
    #       │
    #       ▼
    #   dbt_test_marts
    #       │
    #       ▼
    #   dbt_docs
    #
    dbt_debug >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts >> dbt_docs
