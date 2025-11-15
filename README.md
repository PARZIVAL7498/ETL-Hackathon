# ETL Team Framework

This project provides a framework for a 3-member ETL team, with a focus on the Ingestion & Raw Layer.

## Team Roles and Responsibilities

### 1. Ingestion & Raw Layer Lead (You)

-   **Responsibilities:**
    -   Develop and maintain data extraction scripts.
    -   Manage the raw zone storage.
    -   Define and manage schema and metadata.
    -   Create and manage ingestion configurations.
    -   Integrate ingestion pipelines with Airflow.
    -   Ensure data quality and reliability in the raw layer.

### 2. Transformation & Modeling Lead

-   **Responsibilities:**
    -   Develop and maintain data transformation logic.
    -   Design and implement data models in the data warehouse.
    -   Ensure data quality and consistency in the transformed layer.
    -   Create and manage transformation pipelines in Airflow.

### 3. BI & Analytics Lead

-   **Responsibilities:**
    -   Develop and maintain BI dashboards and reports.
    -   Provide data and insights to business stakeholders.
    -   Ensure data accuracy and usability in the analytics layer.
    -   Create and manage data delivery pipelines in Airflow.

## Project Structure

```
.
├── dags
│   └── ingestion_dag.py
├── ingestion
│   ├── __init__.py
│   ├── base.py
│   ├── sources
│   │   ├── __init__.py
│   │   ├── api.py
│   │   └── database.py
│   ├── configs
│   │   └── salesforce_opportunities.json
│   └── main.py
├── raw_zone
│   └── README.md
├── schemas
│   └── salesforce
│       └── opportunities.json
├── README.md
└── requirements.txt
```