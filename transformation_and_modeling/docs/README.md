# Transformation and Modeling

This repository contains the code and structure for the Transformation & Modeling Lead in a 3-member ETL team.

## Folder Structure

- **src**: Core transformation logic.
  - **python**: Python-based transformation scripts.
  - **spark**: PySpark jobs for distributed processing.
  - **sql**: SQL scripts for database-level transformations.
- **tests**: Automated tests.
  - **unit**: Unit tests for transformation functions.
  - **data_validation**: Data quality and validation tests.
- **config**: Configuration files.
  - **schema_mapping.json**: Schema definitions and mappings.
- **dags**: Airflow DAG definitions for workflow orchestration.
- **data**: Sample data for development and testing.
  - **bronze**: Raw, unprocessed data.
  - **silver**: Cleaned and standardized data.
  - **gold**: Aggregated and modeled data for analytics.
- **notebooks**: Jupyter notebooks for exploratory data analysis.
- **docs**: Documentation.

## Getting Started

1. **Install dependencies:**
   pip install -r requirements.txt

2. **Run transformations:**
   - See scripts in `src` for examples.

3. **Run tests:**
   pytest