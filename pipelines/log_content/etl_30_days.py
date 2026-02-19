import os 
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, when, col
from pyspark.sql.window import Window


# --------------------------
# Spark session
# --------------------------

SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

spark = (
    SparkSession.builder
    .appName("ETL-30Days")
    .master(SPARK_MASTER)
    .config("spark.driver.memory", "4g")
    .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "port": os.environ.get("MYSQL_PORT", "3306"),
    "database": os.environ.get("MYSQL_DATABASE", "mydb"),
    "table": "customer_content_stats",
    "user": os.environ.get("MYSQL_USER", "user"),
    "password": os.environ.get("MYSQL_PASSWORD", "password"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# -------------------------
# Paths (update as needed)
# -------------------------

folder_path = os.environ.get("LOG_CONTENT_RAW_PATH", "/data/raw/log_content/")
save_path = os.environ.get("LOG_CONTENT_DEST_PATH", "/data/destination/log_content/")

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
        .repartition(5)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    print(f"Data saved to {path}")


def import_to_mysql(df, config= MYSQL_CONFIG):
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
        .option("batchsize", 10000)
        .mode("overwrite")
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
        when(col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "Truyen Hinh").\
        when(col("AppName").isin("VOD", "FIMS_RES", "BHD_RES", "VOD_RES", "FIMS", "BHD", "DANET"), "Phim Truyen").\
        when(col("AppName") == "RELAX", "Giai Tri").\
        when(col("AppName") == "CHILD", "Thieu Nhi").\
        when(col("AppName") == "SPORT", "The Thao").\
        otherwise("Error")
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

    # 1) groupBy Contract, Type và sum TotalDuration
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


# -------------------------
# post-transformating (pivot table)
# -------------------------


def most_watch(df):
    """
    Xác định loại nội dung được xem nhiều nhất (MostWatch) cho mỗi khách hàng
    dựa trên tổng mức độ sử dụng của từng nhóm nội dung.

    Hàm này so sánh các cột tổng hợp theo loại nội dung và gán nhãn
    tương ứng với loại có giá trị lớn nhất. Kết quả phản ánh hành vi
    tiêu thụ nội dung nổi trội của khách hàng trong toàn bộ khoảng thời gian
    đã được tổng hợp (ví dụ: 30 ngày).

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào ở mức customer (1 dòng / 1 Contract),
        bắt buộc phải có các cột:
        - Giai Tri
        - Phim Truyen
        - The Thao
        - Thieu Nhi
        - Truyen Hinh

        Các cột trên thường là tổng thời lượng hoặc tổng số lần sử dụng
        nội dung đã được aggregate trước đó.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đầu ra với cột mới:
        - MostWatch : tên loại nội dung có mức sử dụng cao nhất
          đối với mỗi Contract.

    Notes
    -----
    - Hàm sử dụng `greatest()` để xác định giá trị lớn nhất trong các
      cột nội dung.
    - Trong trường hợp nhiều loại nội dung có cùng giá trị lớn nhất,
      kết quả `MostWatch` sẽ phụ thuộc vào thứ tự các điều kiện `when`
      và có thể không hoàn toàn deterministic.
    - Hàm này được thiết kế để sử dụng sau bước tổng hợp dữ liệu
      (ví dụ: sau `groupBy("Contract")`) và không nên áp dụng trực tiếp
      lên dữ liệu raw hoặc daily-level.
    """

    df = df.withColumn("MostWatch", 
                     F.greatest(col("Giai Tri"), col("Phim Truyen"), col("The Thao"), col("Thieu Nhi"), col("Truyen Hinh"))
                     )
    
    df = df.withColumn("MostWatch",
                    when(col("MostWatch") == col("Truyen Hinh"),"Truyen Hinh").
                    when(col("MostWatch") == col("Phim Truyen"),"Phim Truyen").
                    when(col("MostWatch") == col("The Thao"),"The Thao").
                    when(col("MostWatch") == col("Thieu Nhi"),"Thieu Nhi").
                    when(col("MostWatch") == col("Giai Tri"),"Giai Tri")
                    )
    return df


def customer_taste(df):
    """
    Xác định tập hợp các loại nội dung mà khách hàng đã sử dụng
    trong toàn bộ khoảng thời gian tổng hợp (ví dụ: 30 ngày).

    Hàm này tạo cột `Taste` bằng cách kết hợp tên các loại nội dung
    có giá trị sử dụng khác 0, phản ánh sở thích nội dung của khách hàng
    theo nghĩa đã từng sử dụng, không xét mức độ nhiều hay ít.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào ở mức customer (1 dòng / 1 Contract),
        bắt buộc phải có các cột tổng hợp theo loại nội dung:
        - Giai Tri
        - Phim Truyen
        - The Thao
        - Thieu Nhi
        - Truyen Hinh

        Các cột này thường là tổng thời lượng hoặc tổng số lần sử dụng
        nội dung trong một khoảng thời gian xác định.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đầu ra với cột mới:
        - Taste : chuỗi biểu diễn danh sách các loại nội dung mà
          khách hàng đã sử dụng, được nối bằng dấu '-'
          (ví dụ: 'Giai Tri-Phim Truyen-The Thao').

    Notes
    -----
    - Chỉ các loại nội dung có giá trị khác 0 mới được đưa vào `Taste`.
    - Thứ tự các loại nội dung trong chuỗi `Taste` là cố định và
      phụ thuộc vào thứ tự khai báo trong hàm, không phản ánh mức độ ưu tiên
      hay tần suất sử dụng.
    - Hàm này không phân biệt mức độ sử dụng nhiều hay ít, mà chỉ phản ánh
      việc khách hàng có sử dụng loại nội dung đó hay không.
    - Được thiết kế để sử dụng sau bước tổng hợp dữ liệu ở mức customer
      và không nên áp dụng trực tiếp lên dữ liệu raw hoặc daily-level.
    """
    df = df.withColumn("Taste", 
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
    """
    Tính toán mức độ hoạt động (Active) của khách hàng trong cửa sổ thời gian
    nhiều ngày và tổng hợp dữ liệu ở mức customer (Contract).

    Hàm này thực hiện các bước:
    1. Với mỗi `Contract`, đếm số ngày DISTINCT (`Date`) mà contract xuất hiện
       trong toàn bộ tập dữ liệu, không phụ thuộc số thiết bị hay số dòng log
       trong cùng một ngày.
    2. Tổng hợp (aggregate) các chỉ số sử dụng nội dung theo từng `Contract`
       bằng cách cộng dồn giá trị trong nhiều ngày.
    3. Gán giá trị `Active` (số ngày hoạt động) cho mỗi `Contract`.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào ở grain theo ngày (daily-level), trong đó:
        - Mỗi `Contract` có thể xuất hiện nhiều dòng trong một ngày.
        - Bắt buộc phải có các cột:
            * Contract : mã khách hàng
            * Date : ngày phát sinh log (kiểu date hoặc tương đương)
            * Giai Tri, Phim Truyen, The Thao, Thieu Nhi, Truyen Hinh :
              các chỉ số sử dụng nội dung theo ngày

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đã được tổng hợp ở mức customer (1 dòng / 1 Contract),
        bao gồm:
        - Tổng thời lượng sử dụng 30 ngày cho từng loại nội dung
        - Cột `Active` biểu thị số ngày DISTINCT mà contract hoạt động
          trong toàn bộ khoảng thời gian

    Notes
    -----
    - Số ngày hoạt động (`Active`) được tính bằng `countDistinct(Date)`
      để đảm bảo không bị ảnh hưởng bởi việc một contract có nhiều thiết bị
      hoặc nhiều dòng log trong cùng một ngày.
    - Sau khi gọi hàm này, DataFrame kết quả không còn cột `Date` và
      không còn ở grain theo ngày, mà chuyển sang grain theo customer.
    - Hàm này thường được sử dụng để xây dựng bảng customer 30-day summary
      (serving layer / mart) cho mục đích phân tích và báo cáo.
    """
    w = Window.partitionBy("Contract")
    df = df.withColumn("Active", F.count("Date").over(w))
    df = (
        df.groupBy("Contract")
        .agg(
            F.sum("Giai Tri").alias("Giai Tri"),
            F.sum("Phim Truyen").alias("Phim Truyen"),
            F.sum("The Thao").alias("The Thao"),
            F.sum("Thieu Nhi").alias("Thieu Nhi"),
            F.sum("Truyen Hinh").alias("Truyen Hinh"),
            F.max("Active").alias("Active")
        )
    )
    return df


# -------------------------
# Quản lý flow 1-30 days
# List files
# -------------------------


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


# -------------------------
# MISC
# -------------------------


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


# -------------------------
# Control flow
# -------------------------

def control_flow():
    """
    Điều phối toàn bộ luồng ETL dữ liệu log theo ngày và tổng hợp kết quả
    cho cửa sổ thời gian nhiều ngày.

    Hàm đọc lần lượt các file dữ liệu theo ngày, thực hiện các bước làm sạch
    và biến đổi nhẹ (light transform) ở mức daily, sau đó pivot dữ liệu
    về dạng customer-level theo ngày và union tất cả các ngày lại.
    Sau khi hoàn tất vòng lặp, dữ liệu được tổng hợp (heavy transform)
    ở mức customer cho toàn bộ khoảng thời gian và ghi ra storage.

    Quy trình xử lý chi tiết:
    1. Lấy danh sách các file dữ liệu theo ngày và sắp xếp theo thời gian.
    2. Với mỗi file:
        - Đọc dữ liệu JSON daily.
        - Làm phẳng dữ liệu (select `_source.*` nếu tồn tại).
        - Ánh xạ AppName sang nhóm nội dung (Type).
        - Lọc bỏ các bản ghi không hợp lệ (Contract NULL, Contract = '0',
          Type = 'Error').
        - Tổng hợp và pivot dữ liệu về dạng:
          1 dòng / 1 Contract / 1 ngày.
        - Thêm cột `Date` để giữ thông tin ngày phát sinh dữ liệu.
        - Union dữ liệu daily vào DataFrame tổng (`final_df`).
    3. Sau khi xử lý toàn bộ file:
        - Tổng hợp dữ liệu nhiều ngày để tính số ngày hoạt động (`Active`)
          cho mỗi Contract.
        - Xác định loại nội dung được xem nhiều nhất (`MostWatch`).
        - Xác định tập hợp sở thích nội dung (`Taste`).
    4. Ghi kết quả cuối cùng ra storage (CSV).

    Returns
    -------
    None
        Hàm không trả về giá trị, chỉ thực hiện ETL và ghi dữ liệu đầu ra.

    Notes
    -----
    - Hàm giả định danh sách file đầu vào đã được sắp xếp theo thứ tự
      thời gian tăng dần.
    - Sau bước pivot ở mức daily, DataFrame có grain:
        1 dòng / (Contract, Date).
    - Sau các bước tổng hợp cuối cùng, DataFrame có grain:
        1 dòng / Contract.
    - Hàm này phù hợp để xây dựng bảng customer summary cho phân tích,
      báo cáo BI hoặc serving layer trong data warehouse.
    """

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

        # union all days
        if final_df is None:
            final_df = df
        else:
            final_df = final_df.unionByName(df)
        
    final_df = find_active(final_df)
    final_df = most_watch(final_df)
    final_df = customer_taste(final_df)

    print("Saving final output...")
    # save_data(final_df, save_path)
    
    import_to_mysql(final_df)

    print("---- ETL 30 DAYS COMPLETED ----")


if __name__ == "__main__":
    control_flow()