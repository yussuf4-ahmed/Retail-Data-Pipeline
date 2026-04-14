import pandas as pd
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# -----------------------------
# LOAD ENVIRONMENT VARIABLES
# -----------------------------
# This loads values from .env file
# We use this so credentials are NOT hardcoded
load_dotenv()

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")

# File paths
EXTRA_DATA_PATH = "data/extra_data.parquet"

OUTPUT_CLEAN = "output/clean_data.csv"
OUTPUT_AGG = "output/agg_data.csv"


# -----------------------------
# LOGGING CONFIGURATION
# -----------------------------
# Logging helps monitor pipeline execution
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# -----------------------------
# EXTRACT DATA FROM SQL SERVER
# -----------------------------
def extract_grocery_sales():
    """
    Extract grocery sales data from SQL Server database
    This simulates pulling data from production OLTP system
    """

    logging.info("Extracting grocery sales from SQL Server")

    # SQL Server connection string
    connection_string = (
        f"mssql+pyodbc://@{SERVER}/{DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )

    # Create DB connection
    engine = create_engine(connection_string)

    # SQL query
    query = """
    SELECT 
        [index],
        Store_ID,
        Date,
        Dept,
        Weekly_Sales
    FROM grocery_sales
    """

    # Load into pandas dataframe
    df = pd.read_sql(query, engine)

    logging.info(f"Extracted {df.shape[0]} rows")

    return df


# -----------------------------
# LOAD PARQUET FILE
# -----------------------------
def load_extra_data():
    """
    Load complementary data from parquet file
    This simulates external data source (weather, CPI etc.)
    """

    logging.info("Loading extra parquet data")

    df = pd.read_parquet(EXTRA_DATA_PATH)

    logging.info(f"Loaded {df.shape[0]} rows")

    return df


# -----------------------------
# TRANSFORM DATA
# -----------------------------
def transform_data(grocery_sales, extra_data):

    logging.info("Transforming data")

    grocery_sales["Date"] = pd.to_datetime(grocery_sales["Date"])

    grocery_sales = grocery_sales.reset_index(drop=True)
    extra_data = extra_data.reset_index(drop=True)

    # Drop duplicate index column
    if "index" in extra_data.columns:
        extra_data = extra_data.drop(columns=["index"])

    logging.info(f"grocery rows: {len(grocery_sales)}")
    logging.info(f"extra rows: {len(extra_data)}")

    # Safe join
    merged = grocery_sales.join(extra_data, how="left")

    merged["Month"] = merged["Date"].dt.month

    clean_data = merged[
        [
            "Store_ID",
            "Month",
            "Dept",
            "IsHoliday",
            "Weekly_Sales",
            "CPI",
            "Unemployment"
        ]
    ]

    return clean_data


# -----------------------------
# DATA QUALITY CHECKS
# -----------------------------
def data_quality_checks(df):
    """
    Validate data before loading
    This prevents bad data entering warehouse
    """

    logging.info("Running data quality checks")

    # Check for null Store_ID
    assert df["Store_ID"].isnull().sum() == 0, "Null Store_ID found"

    # Check for null Dept
    assert df["Dept"].isnull().sum() == 0, "Null Dept found"

    # Check negative sales
    assert df["Weekly_Sales"].min() >= 0, "Negative sales detected"

    logging.info("Data quality passed")


# -----------------------------
# AGGREGATE DATA
# -----------------------------
def aggregate_data(clean_data):
    """
    Calculate average weekly sales by month
    """

    logging.info("Aggregating monthly sales")

    agg_data = (
        clean_data
        .groupby("Month")["Weekly_Sales"]
        .mean()
        .reset_index()
    )

    return agg_data


# -----------------------------
# CREATE STAR SCHEMA
# -----------------------------
def create_star_schema(clean_data):
    """
    Create dimensional warehouse tables
    """

    logging.info("Creating star schema")

    # Dimension tables
    dim_store = clean_data[["Store_ID"]].drop_duplicates()
    dim_dept = clean_data[["Dept"]].drop_duplicates()
    dim_date = clean_data[["Month"]].drop_duplicates()

    # Fact table
    fact_sales = clean_data.copy()

    # Save tables
    dim_store.to_csv("output/dim_store.csv", index=False)
    dim_dept.to_csv("output/dim_dept.csv", index=False)
    dim_date.to_csv("output/dim_date.csv", index=False)
    fact_sales.to_csv("output/fact_sales.csv", index=False)

    logging.info("Star schema created")


# -----------------------------
# SAVE OUTPUT FILES
# -----------------------------
def save_outputs(clean_data, agg_data):
    """
    Save final outputs to CSV
    """

    clean_data.to_csv(OUTPUT_CLEAN, index=False)
    agg_data.to_csv(OUTPUT_AGG, index=False)

    logging.info("Output files saved")


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run_pipeline():
    """
    Main orchestration function
    Runs entire ETL process
    """

    logging.info("Pipeline started")

    grocery_sales = extract_grocery_sales()
    extra_data = load_extra_data()

    clean_data = transform_data(grocery_sales, extra_data)

    # Run data validation
    data_quality_checks(clean_data)

    agg_data = aggregate_data(clean_data)

    save_outputs(clean_data, agg_data)

    create_star_schema(clean_data)

    logging.info("Pipeline finished successfully")


# Entry point
if __name__ == "__main__":
    run_pipeline()
