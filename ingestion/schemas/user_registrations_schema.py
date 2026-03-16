from pyspark.sql.types import LongType, StringType, StructField, StructType

# Cấu trúc: ,log_user_id,created_date
user_registrations_schema = StructType(
    [
        StructField("_c0", LongType(), True),
        StructField("log_user_id", StringType(), True),
        StructField("created_date", StringType(), True),
    ]
)
