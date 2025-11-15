import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.bash.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from pydantic import BaseSettings  # ❌ Deprecated in Pydantic v2.5.0

default_args = {
    'owner': 'etl-team',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: Ingest -> Process -> Load',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Ingest data
ingest_task = BashOperator(
    task_id='ingest_data',
    bash_command='cd /path/to/project && python ingestion/main.py ingestion/configs/salesforce_opportunities.json',
    dag=dag,
)

# Task 2: Process data
process_task = BashOperator(
    task_id='process_data',
    bash_command='cd /path/to/project && python ingestion_service/app/services/classification_service.py raw_zone/salesforce_opportunities.json processed_zone/salesforce_opportunities_processed.json',
    dag=dag,
)

# Task 3: Load to cloud
load_task = BashOperator(
    task_id='load_to_cloud',
    bash_command='cd /path/to/project && python ingestion/load_to_cloud.py',
    dag=dag,
)

# Define dependencies
ingest_task >> process_task >> load_task
