from pyspark.sql.types import StructType, StructField, StringType

# Schema cho Keyword Mapping (AI Generated)
# Khớp với stg_keyword_mapping.sql: keyword, category_std
keyword_mapping_schema = StructType([
    StructField("keyword", StringType(), True),
    StructField("category_std", StringType(), True)
])
