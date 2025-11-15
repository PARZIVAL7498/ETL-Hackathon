import pytest
import pandas as pd
from src.python.sample_transformation import clean_data

def test_clean_data(tmp_path):
    """
    Tests the clean_data function.
    """

    # Create dummy input CSV
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.parquet"

    df_input = pd.DataFrame({
        "category": ["A", "B", "A", "C"],
        "value": [10, 20, None, 30]
    })
    df_input.to_csv(input_path, index=False)

    # Run the clean_data function (convert paths to strings)
    clean_data(str(input_path), str(output_path))

    # Read cleaned parquet
    df = pd.read_parquet(output_path, engine="pyarrow")

    # 1. No nulls should remain
    assert df["value"].isnull().sum() == 0, "Null values remained after cleaning."

    # 2. Categories are lowercased
    assert df["category"].tolist() == ["a", "b", "a", "c"], \
        "Category column was not standardized to lowercase."

    # 3. Missing value should be replaced with mean of [10, 20, 30]
    expected_mean = (10 + 20 + 30) / 3
    assert expected_mean in df["value"].values, \
        "Missing 'value' was not replaced with the correct mean."
