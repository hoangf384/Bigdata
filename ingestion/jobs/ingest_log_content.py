"""
Job: Ingest Log Content
Layer: Bronze
Source: Local JSON (glob)
Destination: GCS Bronze (Parquet) - Flat schema

Output columns (khớp với DuckDB dbt stg_log_content.sql):
    _id, log_date, contract, mac_address, app_name, total_duration, ingested_at, source_file
"""

import os
import sys
from pyspark.sql import functions as F

# 1. Environment Setup
PROJECT_ROOT = "/app"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from ingestion.utils.spark_utils import get_spark_session

# 2. Configuration
GCS_BUCKET = os.environ.get("GCS_BRONZE_BUCKET", "gs://bigdata-proj")
RAW_SOURCE = os.environ.get("RAW_LOG_CONTENT_PATH", "/app/data/raw/log_content/*.json")
DEST_PATH = f"{GCS_BUCKET}/bronze/log_content"


def ingest():
    """
    Core logic: Read JSON -> Parse JSON fields -> Extract log_date -> Write flat Parquet
    """
    spark = get_spark_session("Bronze-LogContent")

    print(f"--- STARTING INGESTION: Log Content ---")
    print(f"Source: {RAW_SOURCE}")
    print(f"Destination: {DEST_PATH}")

    # 3. Read JSON - chỉ lấy _id và _source (raw string) để parse thủ công
    df = (
        spark.read
        .option("multiLine", "false")
        .json(RAW_SOURCE)
    )

    # 4. Thêm metadata trước khi flatten
    df = (df
          .withColumn("ingested_at", F.current_timestamp())
          .withColumn("source_file", F.input_file_name()))

    # 5. Flatten _source struct & extract log_date từ đường dẫn file
    #    source_file ví dụ: .../log_content/20220401/part-xxx.json -> log_date = 2022-04-01
    df_flat = (
        df
        .withColumn("log_date",
                    F.to_date(
                        F.regexp_extract(F.col("source_file"), r"(\d{8})", 1),
                        "yyyyMMdd"
                    ))
        .withColumn("contract",       F.col("_source.Contract"))
        .withColumn("mac_address",    F.col("_source.Mac"))
        .withColumn("app_name",       F.col("_source.AppName"))
        .withColumn("total_duration", F.col("_source.TotalDuration"))
        .select(
            "_id",
            "log_date",
            "contract",
            "mac_address",
            "app_name",
            "total_duration",
            "ingested_at",
            "source_file",
        )
    )

    # 6. Write flat Parquet to GCS
    print(f"Writing flat Parquet to GCS...")
    (df_flat.write
     .mode("overwrite")
     .parquet(DEST_PATH))

    print(f"--- SUCCESS: Log Content Ingested ---")


if __name__ == "__main__":
    try:
        ingest()
    except Exception as e:
        print(f"!!! INGESTION FAILED: {str(e)}")
        sys.exit(1)


