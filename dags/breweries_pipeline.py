import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


timestamp = datetime.now().strftime("%Y-%m-%d")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "breweries_pipeline",
    default_args=default_args,
    description="Pipeline to process breweries data",
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 20),
    catchup=False
) as dag:

    bronze_task = SparkSubmitOperator(
        task_id="breweries_ingest_data",
        conn_id="spark-conn",
        application="tasks/data_ingestion.py",
        application_args=[
            "--timestamp", timestamp
        ],
        dag=dag
    )    
    
    silver_task = SparkSubmitOperator(
        task_id="breweries_bronze_to_silver",
        conn_id="spark-conn",
        application="tasks/bronze_to_silver.py",
        application_args=[
            "--timestamp", timestamp
        ],        
        dag=dag
    )        
    
    gold_task = SparkSubmitOperator(
        task_id="breweries_silver_to_gold",
        conn_id="spark-conn",
        application="tasks/silver_to_gold.py",
        dag=dag
    )      
    
    bronze_task >> silver_task >> gold_task