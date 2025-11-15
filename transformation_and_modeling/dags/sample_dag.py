from airflow import DAG
from airflow.operators.bash import BashOperator   # FIXED IMPORT
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="sample_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL pipeline: create silver dir, clean data, run spark"
) as dag:

    create_silver_directory = BashOperator(
        task_id="create_silver_directory",
        bash_command=(
            "mkdir -p "
            "/Users/pakshaljain/Desktop/ETL-Hackathon/transformation_and_modeling/data/silver"
        )
    )

    run_python_cleaning = BashOperator(
        task_id="run_python_cleaning",
        bash_command=(
            "python3 "
            "/Users/pakshaljain/Desktop/ETL-Hackathon/transformation_and_modeling/src/python/sample_transformation.py "
            "--input_path /Users/pakshaljain/Desktop/ETL-Hackathon/transformation_and_modeling/data/bronze/sample_data.csv "
            "--output_path /Users/pakshaljain/Desktop/ETL-Hackathon/transformation_and_modeling/data/silver/cleaned_data.parquet"
        )
    )

    run_spark_transformation = BashOperator(
        task_id="run_spark_transformation",
        bash_command=(
            "spark-submit "
            "/Users/pakshaljain/Desktop/ETL-Hackathon/transformation_and_modeling/src/spark/sample_transformation.py"
        )
    )

    create_silver_directory >> run_python_cleaning >> run_spark_transformation
