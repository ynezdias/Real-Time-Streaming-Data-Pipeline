from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'streaming_pipeline',
    default_args=default_args,
    description='ETL pipeline DAG',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    start_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='python /opt/pipeline/kafka/producer.py'
    )

    run_spark_job = BashOperator(
        task_id='run_spark_streaming',
        bash_command='python /opt/pipeline/spark/streaming_job.py'
    )

    write_to_postgres = BashOperator(
        task_id='write_aggregates_to_postgres',
        bash_command='python /opt/pipeline/storage/postgres_writer.py'
    )

    start_producer >> run_spark_job >> write_to_postgres