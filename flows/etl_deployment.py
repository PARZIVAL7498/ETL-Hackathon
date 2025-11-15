"""
Prefect deployment configuration for scheduled ETL pipeline
"""

from prefect import flow
from prefect.deployments import Deployment
from prefect.schedules import CronSchedule
from flows.etl_flow import etl_pipeline

# Create deployment with daily schedule (2 AM)
deployment = Deployment.build(
    flow=etl_pipeline,
    name="etl-pipeline-daily",
    schedule=CronSchedule(cron="0 2 * * *"),  # Daily at 2 AM
    description="Daily ETL pipeline scheduled for 2 AM",
    tags=["etl", "production"],
)

if __name__ == "__main__":
    deployment.apply()
    print("✅ Deployment created successfully!")