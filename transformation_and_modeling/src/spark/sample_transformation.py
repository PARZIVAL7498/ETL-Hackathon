from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, lower
import argparse
import os
import sys

def create_spark_session():
    """
    Creates and returns a Spark session.
    """
    return (
        SparkSession.builder
        .appName("SampleSparkJob")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def clean_data(spark: SparkSession, input_path: str, output_path: str):
    """
    Cleans raw data using Spark.
    """
    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found → {input_path}")
        sys.exit(1)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Ensure numeric column (same logic, safer)
    df = df.withColumn("value", col("value").cast("double"))

    # Handle missing values
    mean_value = df.select(mean(col("value"))).collect()[0][0]
    df = df.fillna({"value": mean_value})

    # Standardize category
    df = df.withColumn("category", lower(col("category")))

    # Ensure directory exists (Spark can fail otherwise)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned data saved to: {output_path}")

def transform_data(spark: SparkSession, input_path: str, output_path: str):
    """
    Transforms cleaned data using Spark.
    """
    if not os.path.exists(input_path):
        print(f"ERROR: Cleaned file not found → {input_path}")
        sys.exit(1)

    df = spark.read.parquet(input_path)

    # Business logic unchanged
    df = df.withColumn("value_doubled", col("value") * 2)

    df.write.mode("overwrite").parquet(output_path)
    print(f"Transformed data saved to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Cleaning + Transformation")
    parser.add_argument("--input_path", required=True, help="Path to raw CSV input")
    parser.add_argument("--output_path", required=True, help="Path for final transformed parquet output")
    args = parser.parse_args()

    spark = create_spark_session()

    # Use two separate paths internally to avoid overwriting
    intermediate_path = args.output_path + "_cleaned"

    # Step 1 → Clean Data (Silver)
    clean_data(spark, args.input_path, intermediate_path)

    # Step 2 → Transform Data (Gold)
    transform_data(spark, intermediate_path, args.output_path)

    spark.stop()

