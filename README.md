# Walmart Retail Data Pipeline

## Overview

This project demonstrates a production-style retail data pipeline that extracts sales data from SQL Server, combines it with external macroeconomic features from a Parquet file, performs data quality validation, and builds an analytics-ready Star Schema for reporting and dashboarding.

The pipeline is fully modular, logged, and designed for orchestration using Apache Airflow.

## Business Problem

Retail organizations often store sales data in relational databases while additional contextual features (holidays, CPI, unemployment, etc.) exist in separate files. This creates challenges for:

- Unified analytics
- Consistent reporting
- Forecasting and trend analysis
- Performance monitoring

This project solves the problem by building a centralized analytics dataset using a modern ETL pipeline.

## Architecture


## Tech Stack

- Python (Pandas, PyArrow)
- SQL Server
- Apache Airflow (DAG design)
- Parquet
- CSV Output Layer
- Logging
- Data Quality Checks

## 📂 Project Structure
```
walmart-retail-data-pipeline/
│
├── airflow/
│ └── walmart_dag.py
│
├── data/
│ └── extra_data.parquet
│
├── dbt_models/
│ └── monthly_sales.sql
│
├── output/
│ ├── clean_data.csv
│ ├── agg_data.csv
│ ├── dim_date.csv
│ ├── dim_dept.csv
│ ├── dim_store.csv
│ └── fact_sales.csv
│
├── scripts/
│ └── pipeline.py
│
├── .env
├── README.md
└── requirements.txt
```


## Pipeline Steps

### 1. Extract
- Pulls grocery_sales data from SQL Server
- Loads external macroeconomic data from Parquet

### 2. Transform
- Converts date fields
- Aligns datasets safely
- Combines retail and economic features
- Creates derived columns (Month, etc.)

### 3. Data Quality Checks
- Null value validation
- Negative sales detection
- Row count validation
- Schema consistency checks

### 4. Aggregation
- Monthly sales metrics
- Department-level summaries
- Store-level performance

### 5. Star Schema Creation

The pipeline generates:

#### Fact Table
- `fact_sales.csv`

#### Dimension Tables
- `dim_date.csv`
- `dim_store.csv`
- `dim_dept.csv`

## Output Datasets

The pipeline generates analytics-ready datasets:

- Cleaned dataset
- Aggregated monthly sales
- Star schema tables
- KPI-ready data

These can be directly connected to Power BI, Tableau, or dashboards.

## Data Quality Strategy

The pipeline includes automated checks:

- Missing value validation
- Sales integrity checks
- Row alignment validation
- Schema enforcement

The pipeline fails early if data quality rules are violated.

## Workflow Orchestration

A production-ready Airflow DAG is included to automate:

- Daily pipeline execution
- Data refresh
- Star schema rebuild
- Analytics output generation

The DAG is deployment-ready for Linux-based environments.

## Business Impact

This pipeline enables:

- Centralized retail analytics
- Performance tracking by store and department
- Economic factor correlation analysis
- Dashboard-ready datasets
- Automated data refresh

## How to Run

1. Install dependencies
   ```bash
   pip install -r requirements.txt
   Run pipeline
   python scripts/pipeline.py
