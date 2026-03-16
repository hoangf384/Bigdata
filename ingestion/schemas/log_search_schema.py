from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Schema cho Log Search (Parquet)
log_search_schema = StructType(
    [
        StructField("log_id", StringType(), True),
        StructField("contract", StringType(), True),
        StructField("mac_address", StringType(), True),
        StructField("search_keyword", StringType(), True),
        StructField("search_timestamp", TimestampType(), True),
    ]
)
