"""
Job: Ingest Viewership Months
Layer: Bronze
Source: Local CSV (Monthly)
Destination: GCS Bronze (Parquet)
"""

import os
import sys
from pyspark.sql import functions as F

# 1. Environment Setup
PROJECT_ROOT = "/app"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from ingestion.utils.spark_utils import get_spark_session
from ingestion.schemas.viewership_schema import viewership_schema

# 2. Configuration
GCS_BUCKET = os.environ.get("GCS_BRONZE_BUCKET", "gs://bigdata-proj")
MONTHS = ["thang4", "thang5", "thang6"]

def ingest_month(month_name):
    """
    Ingest a single month's data.
    """
    spark = get_spark_session(f"Bronze-Viewership-{month_name}")
    
    raw_source = f"/app/data/raw/months/{month_name}.csv"
    dest_path = f"{GCS_BUCKET}/bronze/months/{month_name}"
    
    print(f"--- STARTING INGESTION: {month_name} ---")
    print(f"Source: {raw_source}")
    print(f"Destination: {dest_path}")

    # 3. Read with Schema Enforcement
    df = (spark.read
          .option("header", "true")
          .schema(viewership_schema)
          .csv(raw_source))

    # 4. Metadata Enrichment
    df = (df.withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
            .withColumn("month", F.lit(month_name)))

    # 5. Write to Bronze (Parquet)
    print(f"Writing Parquet to GCS...")
    (df.write
     .mode("overwrite")
     .parquet(dest_path))
    
    print(f"--- SUCCESS: {month_name} Ingested ---")

if __name__ == "__main__":
    try:
        for m in MONTHS:
            ingest_month(m)
        print("Success: All viewership months formatted to Parquet.")
    except Exception as e:
        print(f"!!! INGESTION FAILED: {str(e)}")
        sys.exit(1)

