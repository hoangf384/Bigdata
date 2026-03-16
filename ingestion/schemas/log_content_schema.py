from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

# Schema chính cho file JSON logs (raw read - dùng để đọc, sau đó sẽ flatten trong job)
log_content_schema = StructType(
    [
        StructField("_id",           StringType(), True),
        StructField("_source",       StringType(), True),  # Đọc raw như STRING, parse trong job
    ]
)

# Schema của output Parquet sau khi flatten (ghi lên GCS)
# Khớp với stg_log_content.sql:
#   _id -> log_id, log_date, contract, mac_address, app_name, total_duration
log_content_output_schema = StructType(
    [
        StructField("_id",           StringType(),   True),
        StructField("log_date",      StringType(),   True),  # YYYY-MM-DD từ source_file path
        StructField("contract",      StringType(),   True),
        StructField("mac_address",   StringType(),   True),
        StructField("app_name",      StringType(),   True),
        StructField("total_duration", LongType(),    True),
        StructField("ingested_at",   TimestampType(), True),
        StructField("source_file",   StringType(),   True),
    ]
)
