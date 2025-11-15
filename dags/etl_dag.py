import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Add project root to Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 11, 15),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "hackathon"],
    owner="etl_team",
    description="ETL pipeline for Salesforce data ingestion",
) as dag:
    ingest_task = BashOperator(
        task_id="ingest",
        bash_command=f"cd {PROJECT_ROOT} && python ingestion/main.py ingestion/configs/salesforce_opportunities.json",
        retries=2,
        retry_delay=60,
        doc="Ingest data from Salesforce API",
    )

    process_task = BashOperator(
        task_id="process",
        bash_command=f"cd {PROJECT_ROOT} && python ingestion_service/app/services/classification_service.py raw_zone/salesforce_opportunities.json processed_zone/salesforce_opportunities.json",
        retries=2,
        retry_delay=60,
        doc="Process and classify ingested data",
    )

    load_task = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_ROOT} && python ingestion/load_to_cloud.py processed_zone/salesforce_opportunities.json",
        retries=1,
        retry_delay=60,
        doc="Load processed data to cloud storage",
    )

    ingest_task >> process_task >> load_task
