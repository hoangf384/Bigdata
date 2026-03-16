from pyspark.sql.types import StructType, StructField, StringType

# Schema cho Contracts (Hợp đồng)
# Khớp với stg_contracts.sql: contract, user_id, mac
contracts_schema = StructType([
    StructField("contract", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("mac", StringType(), True)
])
