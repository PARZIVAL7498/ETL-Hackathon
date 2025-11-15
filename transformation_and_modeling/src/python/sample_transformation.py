import pandas as pd
import argparse
import os
import tempfile
import sys

def clean_data(input_path: str, output_path: str):
    """
    Cleans raw data by handling missing values and standardizing formats.
    """
    df = pd.read_csv(input_path)

    # Ensure numeric values (same behavior, more safe)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Handle missing values (same logic)
    df['value'] = df['value'].fillna(df['value'].mean())

    # Standardize category (same output)
    df['category'] = df['category'].astype(str).str.lower()

    df.to_parquet(output_path, engine="pyarrow")


def transform_data(input_path: str, output_path: str):
    """
    Transforms cleaned data by applying business logic.
    """
    df = pd.read_parquet(input_path, engine="pyarrow")

    # Business logic (same meaning)
    df['value_doubled'] = df['value'] * 2

    df.to_parquet(output_path, engine="pyarrow")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and transform data.")
    parser.add_argument("--input_path", required=True, help="Path to the raw input CSV file.")
    parser.add_argument("--output_path", required=True, help="Path to save the final transformed parquet file.")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        intermediate_path = os.path.join(tmpdir, "cleaned.parquet")

        clean_data(args.input_path, intermediate_path)
        transform_data(intermediate_path, args.output_path)

