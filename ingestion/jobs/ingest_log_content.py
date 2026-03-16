"""
Job: Ingest Log Content
Layer: Bronze
Source: Local JSON (glob)
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
from ingestion.schemas.log_content_schema import log_content_schema

# 2. Configuration
GCS_BUCKET = os.environ.get("GCS_BRONZE_BUCKET", "gs://bigdata-proj")
RAW_SOURCE = os.environ.get("RAW_LOG_CONTENT_PATH", "/app/data/raw/log_content/*.json")
DEST_PATH = f"{GCS_BUCKET}/bronze/log_content"

def ingest():
    """
    Core logic: Read -> Enforce Schema -> Add Metadata -> Write Parquet
    """
    spark = get_spark_session("Bronze-LogContent")
    
    print(f"--- STARTING INGESTION: Log Content ---")
    print(f"Source: {RAW_SOURCE}")
    print(f"Destination: {DEST_PATH}")

    # 3. Read with Schema Enforcement
    df = (spark.read
          .schema(log_content_schema)
          .json(RAW_SOURCE))

    # 4. Metadata Enrichment
    df = (df.withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name()))

    # 5. Write to Bronze (Parquet)
    print(f"Writing Parquet to GCS...")
    (df.write
     .mode("overwrite")
     .parquet(DEST_PATH))
    
    print(f"--- SUCCESS: Log Content Ingested ---")

if __name__ == "__main__":
    try:
        ingest()
    except Exception as e:
        print(f"!!! INGESTION FAILED: {str(e)}")
        sys.exit(1)

