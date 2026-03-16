from pyspark.sql.types import StringType, StructField, StructType, ArrayType

# Schema cho Log Search (Khớp chính xác với file Parquet thực tế)
log_search_schema = StructType(
    [
        StructField("eventID", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("keyword", StringType(), True),
        StructField("category", StringType(), True),
        StructField("proxy_isp", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("networkType", StringType(), True),
        StructField("action", StringType(), True),
        StructField("userPlansMap", ArrayType(StringType()), True),
    ]
)
