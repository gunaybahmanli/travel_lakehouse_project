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
    description = "Fetch weather data, upload raw JSON to MinIO, and transform to bronze",
    start_date = datetime(2026, 3, 13),
    schedule_interval = "@daily",
    catchup = False,
    tags = ["weather", "lakehouse", "bronze"]
) as dag:
    
    fetch_weather = BashOperator(
        task_id = "fetch_weather_api",
        bash_command = "python /opt/airflow/scripts/ingest/fetch_weather_api.py",

    )

    upload_weather = BashOperator(
        task_id = "upload_weather_to_minio",
        bash_command = "python /opt/airflow/scripts/ingest/upload_weather_to_minio.py",
    )

    weather_raw_to_bronze = BashOperator(
        task_id="weather_raw_to_bronze",
        bash_command="""
        docker exec travel_spark_master /opt/spark/bin/spark-submit \
          --master local[*] \
          --packages org.apache.hadoop:hadoop-aws:3.3.4 \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
          --conf spark.hadoop.fs.s3a.access.key=minioadmin \
          --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
          /opt/spark/jobs/transform/weather_raw_to_bronze.py {{ ds }}
        """,
    )

    repair_weather_api_table = BashOperator(
        task_id="repair_weather_api_table",
        bash_command="""
        docker exec travel_spark_master /opt/spark/bin/spark-submit \
        --master local[*]  \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        /opt/spark/jobs/transform/repair_weather_api_table.py
        """,
    )

    fetch_weather >> upload_weather >>  weather_raw_to_bronze >> repair_weather_api_table
    