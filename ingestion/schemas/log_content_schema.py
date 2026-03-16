from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Schema cho _source lồng bên trong
source_schema = StructType(
    [
        StructField("Contract", StringType(), True),
        StructField("Mac", StringType(), True),
        StructField("TotalDuration", LongType(), True),
        StructField("AppName", StringType(), True),
    ]
)

# Schema chính cho file JSON logs
log_content_schema = StructType(
    [
        StructField("_index", StringType(), True),
        StructField("_type", StringType(), True),
        StructField("_id", StringType(), True),
        StructField("_score", DoubleType(), True),
        StructField("_source", source_schema, True),
    ]
)
