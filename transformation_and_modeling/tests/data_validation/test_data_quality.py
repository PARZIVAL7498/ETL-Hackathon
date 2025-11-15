import pytest
import pandas as pd

def test_data_quality(tmp_path):
    """
    Tests data quality of the silver dataset.
    """

    # Create dummy silver dataset
    silver_path = tmp_path / "silver.parquet"
    df_expected = pd.DataFrame({
        "category": ["a", "b", "c"],
        "value": [15, 25, 35]
    })
    df_expected.to_parquet(silver_path, engine="pyarrow")

    # Load dataset
    df = pd.read_parquet(silver_path, engine="pyarrow")

    # 1. Check required columns are present
    assert set(["category", "value"]).issubset(df.columns), \
        "Missing required columns in silver data."

    # 2. Check for null values
    assert df.isnull().sum().sum() == 0, \
        "Dataset contains null values."

    # 3. Check category column uniqueness
    assert df["category"].is_unique, \
        "Category column contains duplicates."

    # 4. Check value column is non-negative
    assert df["value"].min() >= 0, \
        "Value column contains negative numbers."
