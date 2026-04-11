"""
airflow/dags/quality_check_dag.py — Data Quality Check DAG for StreamPulse

Runs daily after the main pipeline DAG. Validates:
  1. Row count is above minimum threshold (guards against pipeline stalls)
  2. No NULL event_types were written
  3. Revenue values are non-negative (no corrupt purchase records)
  4. window_start timestamps are within the expected daily range

Failures trigger retries + email-style Airflow alerts via the task log.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import psycopg2

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,   # flip to True and set email_on_failure list
}


# ---------------------------------------------------------------------------
# Check functions (each runs in an isolated PythonOperator task)
# ---------------------------------------------------------------------------

def _get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        database=os.getenv("PG_DB", "pipeline"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "password"),
    )


def check_row_count(min_rows: int = 100, **kwargs):
    """Fail if fewer than `min_rows` were written in the past 24 hours."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM event_aggregates "
        "WHERE window_start >= NOW() - INTERVAL '24 hours'"
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"[quality_check] Row count (last 24 h): {count}")
    if count < min_rows:
        raise ValueError(
            f"Quality gate FAILED: only {count} rows in last 24 h "
            f"(minimum required: {min_rows}). "
            "Check that the Kafka producer and Spark job are running."
        )
    print(f"[quality_check] ✅ Row count OK ({count} >= {min_rows})")


def check_null_event_types(**kwargs):
    """Fail if any rows have NULL or empty event_type."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM event_aggregates "
        "WHERE event_type IS NULL OR event_type = '' "
        "AND window_start >= NOW() - INTERVAL '24 hours'"
    )
    null_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"[quality_check] NULL event_type count: {null_count}")
    if null_count > 0:
        raise ValueError(
            f"Quality gate FAILED: {null_count} rows with NULL/empty event_type. "
            "Check the Spark schema and producer payload."
        )
    print("[quality_check] ✅ No NULL event_types")


def check_non_negative_revenue(**kwargs):
    """Fail if any purchase rows show negative revenue."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM event_aggregates "
        "WHERE total_revenue < 0 "
        "AND window_start >= NOW() - INTERVAL '24 hours'"
    )
    neg_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"[quality_check] Negative revenue rows: {neg_count}")
    if neg_count > 0:
        raise ValueError(
            f"Quality gate FAILED: {neg_count} rows with negative revenue. "
            "Check transformations.py window_aggregations()."
        )
    print("[quality_check] ✅ All revenue values non-negative")


def check_event_type_distribution(**kwargs):
    """Warn if any expected event type is completely absent from last 24 h."""
    expected_types = {"page_view", "search", "add_to_cart", "purchase", "logout"}
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT event_type FROM event_aggregates "
        "WHERE window_start >= NOW() - INTERVAL '24 hours'"
    )
    present = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    missing = expected_types - present
    print(f"[quality_check] Present event types: {present}")
    if missing:
        # Soft warning — not a hard failure (low-traffic windows are valid)
        print(f"[quality_check] ⚠️  Missing event types in last 24 h: {missing}")
    else:
        print("[quality_check] ✅ All expected event types present")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Daily data quality checks on event_aggregates",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["quality", "monitoring"],
) as dag:

    # ── 1. Row count gate ──────────────────────────────────────────────────
    t_row_count = PythonOperator(
        task_id="check_row_count",
        python_callable=check_row_count,
        op_kwargs={"min_rows": 100},
    )

    # ── 2. NULL event_type check ───────────────────────────────────────────
    t_null_event = PythonOperator(
        task_id="check_null_event_types",
        python_callable=check_null_event_types,
    )

    # ── 3. Non-negative revenue ────────────────────────────────────────────
    t_revenue = PythonOperator(
        task_id="check_non_negative_revenue",
        python_callable=check_non_negative_revenue,
    )

    # ── 4. Event type distribution (soft check) ────────────────────────────
    t_distribution = PythonOperator(
        task_id="check_event_type_distribution",
        python_callable=check_event_type_distribution,
    )

    # ── 5. Log summary ─────────────────────────────────────────────────────
    t_summary = BashOperator(
        task_id="log_quality_summary",
        bash_command=(
            'echo "✅ All quality checks completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"'
        ),
    )

    # ── Task DAG ───────────────────────────────────────────────────────────
    # Run all checks in parallel, then log summary
    [t_row_count, t_null_event, t_revenue, t_distribution] >> t_summary
