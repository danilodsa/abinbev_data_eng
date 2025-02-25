import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from tasks.data_ingestion import ingest_breweries_data
from tasks.bronze_to_silver import bronze_to_silver
from tasks.silver_to_gold import silver_to_gold



timestamp = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "breweries_pipeline",
    default_args=default_args,
    description="Pipeline to process breweries data",
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 20),
    catchup=False
) as dag:
  
    bronze_task = PythonOperator(
        task_id="breweries_ingest_data",
        python_callable=ingest_breweries_data,
        op_kwargs={"output_path": "data/bronze/breweries/json/", "timestamp": timestamp},
    )
    
    silver_task = PythonOperator(
        task_id="breweries_bronze_to_silver",
        python_callable=bronze_to_silver,
        op_kwargs={"bronze_path": "data/bronze/breweries/json/", "silver_path": "data/silver/breweries/", "timestamp": timestamp},
    )
    
    gold_task = PythonOperator(
        task_id="breweries_silver_to_gold",
        python_callable=silver_to_gold,
        op_kwargs={"silver_path": "data/silver/breweries/", "gold_path": "data/gold/breweries/"},
    )
    
    bronze_task >> silver_task >> gold_task