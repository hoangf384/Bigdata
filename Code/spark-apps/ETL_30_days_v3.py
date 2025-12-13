import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, when, col
from pyspark.sql.window import Window


# ------------------------
# Spark session
# ------------------------

spark = (
    SparkSession.builder
    .appName("ETL-30Days-v2")
    .master("spark://spark-master:7077")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

MYSQL_CONFIG = {
    "host": "localhost",
    "port": "3306",
    "database": "etl_data",
    "table": "customer_content_stats",
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# -------------------------
# Paths (update as needed)
# -------------------------

folder_path = "/data/raw/log_content/"
save_path = "/data/destination/log_content/"

# -------------------------
# Helpers: IO
# -------------------------


def read_data(spark, path):
    """
    Đọc dữ liệu JSON vào Spark DataFrame.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        SparkSession đang hoạt động, dùng để đọc dữ liệu.
    path : str
        Đường dẫn tới file JSON hoặc thư mục chứa nhiều file JSON.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame chứa dữ liệu JSON với schema được Spark tự suy luận.
    """
    df = spark.read.json(path)
    return df


def save_data(result, path):
    """
    Ghi Spark DataFrame ra file CSV.

    Parameters
    ----------
    result : pyspark.sql.DataFrame
        DataFrame chứa dữ liệu đầu ra cần ghi.
    path : str
        Đường dẫn thư mục lưu file CSV kết quả.

    Returns
    -------
    None
        Hàm không trả về giá trị, chỉ thực hiện ghi dữ liệu ra đĩa.
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


def import_to_mysql(df, config):
    """
    Ghi Spark DataFrame vào MySQL bằng JDBC.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame chứa dữ liệu cần ghi vào MySQL.
    config : dict
        Cấu hình kết nối MySQL, bao gồm host, port, database,
        table, user, password và driver.

    Returns
    -------
    None
        Hàm không trả về giá trị.
    """

    url = f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}"

    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("driver", config["driver"])
        .option("dbtable", config["table"])
        .option("user", config["user"])
        .option("password", config["password"])
        .mode("append")
        .save()
    )

    print("Data imported successfully to MySQL")

# -------------------------
# Transform steps
# -------------------------


def select_fields(df):
    """
    Chọn các trường dữ liệu từ cột `_source` nếu dữ liệu JSON có cấu trúc lồng.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào, có thể chứa cột `_source` hoặc đã là dữ liệu phẳng.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đã được làm phẳng bằng cách chọn `_source.*` nếu cột
        `_source` tồn tại; ngược lại trả về DataFrame ban đầu.
    """
    
    if "_source" in df.columns:
        return df.select("_source.*")
    else:
        return df


def transform_category(df):
    """
    Ánh xạ giá trị AppName sang nhóm nội dung (Type).

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào, bắt buộc phải có cột `AppName`.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đầu ra với cột mới `Type`, biểu diễn nhóm nội dung
        tương ứng với từng giá trị `AppName`.
    """
    return df.withColumn(
        "Type",
        when(col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "Truyen Hinh")
        .when(col("AppName").isin("VOD", "FIMS_RES", "BHD_RES", "VOD_RES", "FIMS", "BHD", "DANET"), "Phim Truyen")
        .when(col("AppName") == "RELAX", "Giai Tri")
        .when(col("AppName") == "CHILD", "Thieu Nhi")
        .when(col("AppName") == "SPORT", "The Thao")
        .otherwise("Error")
    )


def pivot_table(df):
    """
    Tổng hợp và xoay bảng dữ liệu theo nhóm nội dung.

    Hàm chuyển dữ liệu từ dạng dài (long format):
        (Contract, Type, TotalDuration)
    sang dạng rộng (wide format):
        1 dòng / Contract, mỗi cột là một loại nội dung.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào, bắt buộc phải có các cột:
        `Contract`, `Type`, `TotalDuration`.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đã được tổng hợp và pivot, trong đó:
        - Mỗi dòng tương ứng với một `Contract`.
        - Mỗi cột tương ứng với một giá trị `Type`.
        - Giá trị ô là tổng `TotalDuration` của từng loại nội dung.

    Notes
    -----
    - Dữ liệu được tổng hợp bằng `SUM(TotalDuration)` theo
      từng cặp (`Contract`, `Type`).
    - Các giá trị bị thiếu sau khi pivot sẽ được điền bằng 0.
    - Output DataFrame có grain là 1 dòng trên mỗi `Contract`,
      phù hợp cho các bước phân tích OLAP tiếp theo.
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
    return pivoted


def most_watch(df):
    # define most_watch
    df= df.withColumn("MostWatch", 
                     F.greatest(col("Giai Tri"), col("Phim Truyen"), col("The Thao"), col("Thieu Nhi"), col("Truyen Hinh"))
                     )
    df= df.withColumn("MostWatch",
                    when(col("MostWatch") == col("Truyen Hinh"),"Truyen Hinh").
                    when(col("MostWatch") == col("Phim Truyen"),"Phim Truyen").
                    when(col("MostWatch") == col("The Thao"),"The Thao").
                    when(col("MostWatch") == col("Thieu Nhi"),"Thieu Nhi").
                    when(col("MostWatch") == col("Giai Tri"),"Giai Tri")
                    )
    return df


def customer_taste(df):
    # define customer_taste
    df=df.withColumn("Taste", 
                     F.concat_ws("-",
                                 when(col("Giai Tri") != 0, lit("Giai Tri")),
                                 when(col("Phim Truyen") != 0, lit("Phim Truyen")),
                                 when(col("The Thao") != 0, lit("The Thao")),
                                 when(col("Thieu Nhi") != 0, lit("Thieu Nhi")),
                                 when(col("Truyen Hinh") != 0, lit("Truyen Hinh"))
                                 )
                    )
    return df


def find_active(df):
    windowspec = Window.partitionBy("Contract")
    df = df.withColumn("Active",F.count("Date").over(windowspec))
    df = df.drop("Date")
    df = df.withColumn("Active",when(col("Active") > 4,"High").otherwise("Low"))
    df = df.groupBy("Contract").agg(
    F.sum("Giai Tri").alias("Total_Giai_Tri"),
    F.sum("Phim Truyen").alias("Total_Phim_Truyen"),
    F.sum("The Thao").alias("Total_The_Thao"),
    F.sum("Thieu Nhi").alias("Total_Thieu_Nhi"),
    F.sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
    F.first("MostWacth").alias("MostWacth"),
    F.first("Taste").alias("Taste"),
    F.first("Active").alias("Active")
)
    return df

# Quản lý flow 1-30 days
# List files
def list_files_sorted(path):
    """
    Liệt kê và sắp xếp các file JSON trong một thư mục.

    Parameters
    ----------
    path : str
        Đường dẫn tới thư mục chứa các file JSON.

    Returns
    -------
    list of str
        Danh sách đường dẫn đầy đủ tới các file JSON,
        được sắp xếp theo thứ tự tăng dần.
    """
    files = [
        os.path.join(path, f)
        for f in os.listdir(path)
        if f.endswith(".json") and os.path.isfile(os.path.join(path, f))
    ]
    return sorted(files)

#-------------------
# MISC
#-------------------

def extract_date_from_filename(path):
    """
    Trích xuất ngày từ tên file JSON và chuyển sang kiểu date.

    Parameters
    ----------
    path : str
        Đường dẫn tới file JSON, với tên file có định dạng `YYYYMMDD.json`.

    Returns
    -------
    datetime.date
        Giá trị ngày được trích xuất từ tên file, ở dạng `datetime.date`.
    """
    
    base = os.path.basename(path)      # 20220401.json
    date_str = base.split(".")[0]   # 20220401
    return datetime.strptime(date_str, "%Y%m%d").date()

# ------


def main():
    all_files = list_files_sorted(folder_path)
    print("Files to process:", all_files)

    final_df = None

    for file in all_files:
        print(f"Processing file: {file}")

        # Extract date
        date_str = extract_date_from_filename(file)

        # 1 day ETL
        df = read_data(spark, file)
        df = select_fields(df)
        df = transform_category(df)
        
        # 2. fillter
        df = df.filter((col("Contract").isNotNull()) & (col("Contract") != "0"))
        df = df.filter(col("Type") != "Error")
        
        # summarize, pivot
        df = pivot_table(df)
        # add date column
        df = df.withColumn("Date", F.lit(date_str))
        # them most watch, customer taste
        df = most_watch(df)
        df = customer_taste(df)

        # union all days
        if final_df is None:
            final_df = df
        else:
            final_df = final_df.unionByName(df)

    print("Saving final output...")
    save_data(final_df, save_path)

    print("---- ETL 30 DAYS COMPLETED ----")

if __name__ == "__main__":
    main()