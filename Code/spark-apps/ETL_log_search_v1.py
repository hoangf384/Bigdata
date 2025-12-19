from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import os
from datetime import datetime


# -----------------------------------
# init
# -----------------------------------


spark = (
    SparkSession.builder
    .appName("Local-ETL-Test")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024) # 256 * 1024 * 1024 bytes
    .config("spark.sql.shuffle.partitions", "200") # 200 partitions for shuffle operations
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# -----------------------------------
# Folder, path,...
# -----------------------------------


folder_path = f"/data/raw/log_search/"
date_file = sorted(os.listdir(folder_path))
save_path = f"/data/destination/log_search/"


# -----------------------------------
# helpers: IO
# -----------------------------------


def read_data(path, sub_folder):
    """
    Đọc toàn bộ file parquet trong một thư mục con và trả về Spark DataFrame.

    Parameters
    ----------
    path : str
        Đường dẫn thư mục gốc chứa dữ liệu.
    sub_folder : str
        Tên thư mục con nằm trong `path`, nơi chứa các file.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame được tạo bằng cách đọc tất cả các file
        `*.parquet` trong thư mục `path/sub_folder`.
    """

    path = os.path.join(path, sub_folder, "*.parquet")
    
    df = spark.read.parquet(path)
    return df


def save_data(df, path):
    """
    Ghi Spark DataFrame ra file CSV.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame chứa dữ liệu cần ghi ra file.
    path : str, optional
        Đường dẫn thư mục lưu kết quả CSV.
        Mặc định là giá trị của biến `save_path`.

    Returns
    -------
    None
        Hàm không trả về giá trị, chỉ thực hiện ghi dữ liệu ra đĩa.
    """
    
    (
        df
        .repartition(4)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    print(f"Data saved to {path}")


# -----------------------------------
# light transform
# -----------------------------------


def datetime_transform(df, date_value):
    """
    Chuẩn hoá và gắn thông tin ngày vào DataFrame dựa trên giá trị ngày 
    được trích xuất từ tên thư mục hoặc tên file.

    lấy giá `month` từ tên thư mục và trả về.
    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame đầu vào, có thể chứa cột `datetime` (sẽ bị loại bỏ).
    date_value : str
        Chuỗi ngày có định dạng `YYYYMMDD`
        (ví dụ: '20241201').

    Returns
    -------
    tuple
        (df, month_value) trong đó:
        - df : pyspark.sql.DataFrame  
          DataFrame sau khi:
            * drop cột `datetime`
            * thêm cột `date` (kiểu date)
        - month_value : int  
          Giá trị `month` được trích xuất từ `date_value`.
    """
    
    dt = datetime.strptime(date_value, "%Y%m%d")
    date_value = dt.date()
    month_value = dt.month
    
    df = df.drop("datetime")
    df = df.withColumn("date", F.lit(date_value))
    return df, month_value


# -----------------------------------
# heavy transform
# -----------------------------------


def most_watch(final_df):
    """
    Xác định từ khoá (keyword) được tìm kiếm nhiều nhất cho mỗi người dùng.

    Hàm thực hiện các bước:
    1. Loại bỏ các bản ghi có `user_id` bị NULL.
    2. Đếm số lần xuất hiện của mỗi cặp (`user_id`, `keyword`).
    3. Tạo cửa sổ (window) theo `user_id`, sắp xếp theo số lần tìm kiếm
       giảm dần.
    4. Gán số thứ tự (`row_number`) cho từng keyword của mỗi user.
    5. Chọn keyword có tần suất cao nhất (`rn = 1`) cho mỗi `user_id`.
    6. Đổi tên cột `keyword` thành `mostWatch` và trả về kết quả cuối cùng.

    Parameters
    ----------
    final_df : pyspark.sql.DataFrame
        DataFrame đầu vào, bắt buộc phải có các cột:
        - user_id : định danh người dùng
        - keyword : từ khoá tìm kiếm

        Mỗi dòng đại diện cho một lần người dùng thực hiện tìm kiếm.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame đầu ra với grain:
        - 1 dòng / 1 user_id

        Bao gồm các cột:
        - user_id : định danh người dùng
        - mostWatch : từ khoá được tìm kiếm nhiều nhất

    Notes
    -----
    - Hàm sử dụng `count()` để đo tần suất tìm kiếm của từng keyword.
    - Trong trường hợp một người dùng có nhiều keyword có cùng số lần
      tìm kiếm cao nhất, kết quả được chọn sẽ phụ thuộc vào thứ tự
      sắp xếp nội bộ của Spark (không hoàn toàn deterministic).
    - Hàm này phù hợp để sử dụng sau bước làm sạch dữ liệu log
      (log search, event log, v.v.).
    """

    final_df = final_df.where(col("user_id").isNotNull())

    final_df = (
        final_df
        .groupBy("user_id", "keyword")
        .count()
    )

    w = Window.partitionBy("user_id").orderBy(col("count").desc())

    final_df = (
        final_df
        .withColumn("rn", F.row_number().over(w))
        .where(col("rn") == 1)
        .select("user_id", col("keyword").alias("mostWatch"))
    )

    return final_df


# -----------------------------------
# main task
# -----------------------------------


def control_flow():
    """
    Điều phối luồng xử lý ETL theo ngày và tổng hợp dữ liệu theo tháng.

    Hàm đọc lần lượt các file dữ liệu theo ngày, thực hiện transform,
    sau đó union dữ liệu theo từng tháng. Khi phát hiện tháng thay đổi,
    dữ liệu của tháng trước sẽ được tổng hợp, ghi ra storage và
    giải phóng khỏi bộ nhớ trước khi tiếp tục xử lý tháng mới.

    Quy trình xử lý:
    1. Khởi tạo `final_df` để lưu dữ liệu tích luỹ theo tháng.
    2. Lặp qua danh sách file dữ liệu theo ngày (`date_file`).
    3. Với mỗi file:
       - Đọc dữ liệu daily.
       - Chuẩn hoá ngày và xác định tháng tương ứng.
       - Nếu cùng tháng hiện tại: union vào `final_df`.
       - Nếu khác tháng: 
           * Tổng hợp dữ liệu tháng cũ.
           * Ghi kết quả ra thư mục đích theo partition tháng.
           * Reset biến để bắt đầu tháng mới.
    4. Sau khi kết thúc vòng lặp, flush dữ liệu của tháng cuối cùng.

    Returns
    -------
    None
        Hàm không trả về giá trị, chỉ thực hiện ETL và ghi dữ liệu ra storage.

    Notes
    -----
    - Hàm giả định danh sách `date_file` đã được sắp xếp theo thứ tự thời gian tăng dần.
    - Dữ liệu được ghi ra theo cấu trúc thư mục:
        `save_path/month=<month_value>`
      phù hợp với mô hình partition trong data warehouse / data lake.
    - Việc flush theo tháng giúp:
        * Giảm dung lượng dữ liệu giữ trong bộ nhớ.
        * Phù hợp xử lý dữ liệu lớn theo batch (daily → monthly).
    - Hàm `most_watch()` được áp dụng ở mức dữ liệu tháng
      trước khi ghi ra storage.

    Examples
    --------
    >>> control_flow()
    """
    
    final_df = None
    df = None

    current_month = None
    month_value = None

    for file in date_file:
        print(f"Processing daily file: {file}")

        # read + transform
        df = read_data(folder_path, file)
        df, month_value = datetime_transform(df, file)

        # first batch
        if final_df is None:
            final_df = df
            current_month = month_value
            continue

        # same month -> union
        if month_value == current_month:
            final_df = final_df.unionByName(df)


        # month changed -> flush old month
        else:
            print(f"Flushing month {current_month}")

            final_df = most_watch(final_df)

            path = f"{save_path}/month={current_month}" # month_{} -> month={} chatgpt noi dw lam nhu the
            save_data(final_df, path)

            # reset for new month
            final_df = df
            current_month = month_value

    # flush last month
    if final_df is not None:
        print(f"Flushing month {current_month}")

        final_df = most_watch(final_df)

        path = f"{save_path}/month={current_month}"
        save_data(final_df, path)

    print("ETL task complete")

    
if __name__ == "__main__":
    control_flow()