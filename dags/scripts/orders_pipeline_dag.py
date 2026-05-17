from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mci2026_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'orders_realtime_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *', # Berjalan setiap 10 menit
    catchup=False,
    max_active_runs=1,
    description='Micro-batching Orders API -> Spark -> ClickHouse'
) as dag:

    ingest_stream = BashOperator(
        task_id='fetch_orders_api',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders_stream.py'
    )

    process_analytics = BashOperator(
        task_id='spark_process_and_load_clickhouse',
        bash_command='python /opt/airflow/dags/scripts/process_orders_spark.py'
    )

    ingest_stream >> process_analytics