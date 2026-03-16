"""
Job: Ingest Keyword Mapping
Layer: Bronze
Source: Local JSON
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
from ingestion.schemas.keyword_mapping_schema import keyword_mapping_schema

# 2. Configuration
GCS_BUCKET = os.environ.get("GCS_BRONZE_BUCKET", "gs://bigdata-proj")
RAW_SOURCE = os.environ.get("RAW_MAPPING_PATH", "/app/data/raw/mapping/mapping.json")
DEST_PATH = f"{GCS_BUCKET}/bronze/keyword_mapping"

def ingest():
    """
    Core logic: Read -> Enforce Schema -> Add Metadata -> Write Parquet
    """
    spark = get_spark_session("Bronze-KeywordMapping")
    
    print(f"--- STARTING INGESTION: Keyword Mapping ---")
    print(f"Source: {RAW_SOURCE}")
    print(f"Destination: {DEST_PATH}")

    # 3. Read with Schema Enforcement
    df = (spark.read
          .schema(keyword_mapping_schema)
          .json(RAW_SOURCE))

    # 4. Metadata Enrichment
    df = (df.withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name()))

    # 5. Write to Bronze (Parquet)
    print(f"Writing Parquet to GCS...")
    (df.write
     .mode("overwrite")
     .parquet(DEST_PATH))
    
    print(f"--- SUCCESS: Keyword Mapping Ingested ---")

if __name__ == "__main__":
    try:
        ingest()
    except Exception as e:
        print(f"!!! INGESTION FAILED: {str(e)}")
        sys.exit(1)

