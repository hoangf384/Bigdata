from pyspark.sql.types import StructType, StructField, StringType, LongType

# Schema cho dữ liệu lượt xem theo tháng (thang4, thang5, thang6)
# Khớp với stg_april.sql: user_id, contentName, screenName, TotalView
viewership_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("contentName", StringType(), True),
    StructField("screenName", StringType(), True),
    StructField("TotalView", LongType(), True)
])
