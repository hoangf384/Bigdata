from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

# -------------------------
# Spark session
# -------------------------
spark = (
    SparkSession.builder
    .appName("ETL-1day")
    .master("spark://spark-master:7077")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)  # 256MB
    .config("spark.sql.shuffcle.partitions", "200") # 200 partitions for shuffle
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# -------------------------
# Paths (update as needed)
# -------------------------
folder_path = "/data/raw/log_content/20220401.json"
save_path = "/data/destination/log_content/20220401/"

# -------------------------
# Helpers: IO
# -------------------------
def read_data(path):
    """
    Đọc JSON từ path (có thể là file hoặc folder)
    """
    df = spark.read.json(path)
    return df


def save_data(result, path):
    """
    Ghi CSV với 1 file đầu ra. Dùng overwrite.
    """
    (
        result
        .repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    print(f"Data saved to {path}")

# -------------------------
# Transform steps
# -------------------------
def select_fields(df):
    """
    Lấy phần _source.* nếu input nested
    """
    # bảo đảm cột _source tồn tại trước khi select
    if "_source" in df.columns:
        return df.select("_source.*")
    else:
        return df


def transform_category(df):
    """
    Map AppName -> Type (category)
    """
    return df.withColumn(
        "Type",
        when(col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "Truyền Hình")
        .when(col("AppName").isin("VOD", "FIMS_RES", "BHD_RES", "VOD_RES", "FIMS", "BHD", "DANET"), "Phim Truyện")
        .when(col("AppName") == "RELAX", "Giải Trí")
        .when(col("AppName") == "CHILD", "Thiếu Nhi")
        .when(col("AppName") == "SPORT", "Thể Thao")
        .otherwise("Error")
    )


def pivot_table(df):
    """
    Tóm tắt và pivot: (Contract, Type, TotalDuration) -> wide table
    """
    # 1) groupBy Contract,Type và sum TotalDuration
    summary = (
        df.groupBy("Contract", "Type")
          .agg(F.sum(F.col("TotalDuration").cast("long")).alias("TotalDuration"))
    )

    # 2) pivot để tạo cột cho mỗi Type
    pivoted = (
        summary.groupBy("Contract")
               .pivot("Type")
               .sum("TotalDuration")
               .na.fill(0)
    )

    # 3) rename cột từ tiếng VN sang tên chuẩn (nếu tồn tại)
    rename_map = {
        "Truyền Hình": "TVDuration",
        "Phim Truyện": "MovieDuration",
        "Thiếu Nhi": "ChildDuration",
        "Giải Trí": "RelaxDuration",
        "Thể Thao": "SportDuration"
    }

    for old_name, new_name in rename_map.items():
        if old_name in pivoted.columns:
            pivoted = pivoted.withColumnRenamed(old_name, new_name)

    return pivoted

# -------------------------
# Main pipeline
# -------------------------
def main(path_in, path_out):
    print("---- Read data ----")
    df_raw = read_data(path_in)
    df_raw.show(3, truncate=False)

    print("---- Select fields ----")
    df = select_fields(df_raw)
    df.show(3, truncate=False)

    print("---- Transform category ----")
    df = transform_category(df)
    df.show(3, truncate=False)

    # loại bỏ contract = '0' và Type = 'Error' nếu muốn
    df = df.filter((col("Contract").isNotNull()) & (col("Contract") != "0"))
    df = df.filter(col("Type") != "Error")

    print("---- Calculate statistics (pivot) ----")
    stats = pivot_table(df)
    stats.show(10, truncate=False)
    
    print("---- Save result ----")
    save_data(stats, path_out)

    print("Task finished")
    return stats

# run
if __name__ == "__main__":
    main(folder_path, save_path)
