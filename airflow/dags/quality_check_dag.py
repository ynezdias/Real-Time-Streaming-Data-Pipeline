from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'you',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_null_values():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        database=os.getenv("PG_DB", "pipeline"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "password")
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM event_aggregates WHERE event_type IS NULL OR window_start IS NULL;")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    if result > 0:
        raise ValueError(f"Data quality check failed: Found {result} rows with NULL event_type or window_start.")
    print(f"Data quality check passed: 0 nulls found.")

def check_row_count():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        database=os.getenv("PG_DB", "pipeline"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "password")
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM event_aggregates;")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    if result == 0:
        raise ValueError("Data quality check failed: Table event_aggregates is empty.")
    print(f"Data quality check passed: {result} rows found.")

with DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Perform data quality checks on PostgreSQL aggregates',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    null_check = PythonOperator(
        task_id='check_null_values',
        python_callable=check_null_values
    )

    row_count_check = PythonOperator(
        task_id='check_row_count',
        python_callable=check_row_count
    )

    row_count_check >> null_check
