from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id = "weather_ingestion_dag",
    default_args = default_args,
    description = "A DAG to ingest weather data and upload to Minio",
    start_date = datetime(2026, 3, 12),
    schedule_interval = "@daily",
    catchup = False,
) as dag:
    
    fetch_weather = BashOperator(
        task_id = "fetch_weather_api",
        bash_command = "python /opt/airflow/scripts/ingest/fetch_weather_api.py",

    )

    upload_weather = BashOperator(
        task_id = "upload_weather_to_minio",
        bash_command = "python /opt/airflow/scripts/ingest/upload_weather_to_minio.py",
    )

    fetch_weather >> upload_weather