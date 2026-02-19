from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from pyspark.sql.functions import lit, when, col

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------
# init, spark, config and loging
# --------------------------

spark = (
    SparkSession.builder
    .appName("post-enrich")
    .master("spark://spark-master:7077")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

MYSQL_CONFIG = {
    "host": "mysql",
    "port": "3306",
    "database": "mydb",
    "table": "customer_search_stats",
    "user": "user",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# --------------------------
# Folder
# --------------------------

log_search = "/data/destination/log_search/"

mapping_file = log_search + "/category/" # json

m6 = log_search + "month=6/" # csv 
m7 = log_search + "month=7/" # csv


# --------------------------
# IO helpers
# --------------------------

def read_data(dtype: str, path: str) -> DataFrame:
    """
    Docstring for read_data
    
    :param dtype: Description
    :type dtype: str
    :param path: Description
    :type path: str
    :return: Description
    :rtype: DataFrame
    """
    
    data = None # Khai bao DF

    if dtype == "csv":
        data = spark.read.option("header", True).csv(path + "*.csv")
        return data
    
    elif dtype == "json":
        data = spark.read.json(path + "*.json")
        return data
    
    else:
        raise ValueError(f"Unsupported dtype: {dtype}")


def save_data(result: DataFrame, path: str) -> None:
    """
    Ghi Spark DataFrame ra file CSV.

    Parameters
    ----------
    result : DataFrame
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
        .mode("append")
        .option("header", True)
        .csv(path)
    )
    logger.info(f"Result save into {path}")


def import_to_mysql(result: DataFrame, config: dict) -> None:
    """
    Ghi Spark DataFrame vào MySQL bằng JDBC.

    Parameters
    ----------
    result : DataFrame
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
        result.write
        .format("jdbc")
        .option("url", url)
        .option("driver", config["driver"])
        .option("dbtable", config["table"])
        .option("user", config["user"])
        .option("password", config["password"])
        .option("batchsize", 10000)
        .mode("overwrite") # Full rebuild
        .save()
    )

    logger.info("Data imported successfully to MySQL")

# --------------------------
# Transform data
# --------------------------

def join_data(data1: DataFrame, data2: DataFrame, key1: str, key2: str, select_cols: list) -> DataFrame:
    """
    join function
    join data2 với data1 dựa trên data1.key1 và data2.key2
    lựa chọn cột trả về
    * dùng dể unpack
    """
    a = data1.alias("a")
    b = data2.alias("b")
    
    result = a.join(
        b,
        a[key1] == b[key2],
        how="left"
    )
    result = result.select(*select_cols)
    
    return result

def customertaste(data: DataFrame) -> DataFrame:
    """
    Docstring for customertaste
    
    :param data: Description
    :type data: DataFrame
    :return: Description
    :rtype: DataFrame
    """
    result = data.withColumn(
        "taste",
        when(col("category_m6") == col("category_m7"), 
             lit("Unchanged"))
            .otherwise(lit("Changed"))
    )
    return result

# ---------------------
# Control flow
# ---------------------

def control_flow():
    
    logger.info("Reading mapping data")
    
    mapping_table = read_data("json", mapping_file)
    mapping_table.show(10, truncate= False)
    

    logger.info("Reading log search data")
    
    m6_table = read_data("csv", m6)
    m7_table = read_data("csv", m7)
    m6_table.show(10, truncate= False)
    m7_table.show(10, truncate= False)

    logger.info("Transform step")

    select_cols = [
        col("a.user_id"),
        col("a.mostWatch"),
        col("b.category_std")
    ]
    logger.info("select_cols = %s", select_cols)
    
    logger.info("mapping June and July search table with mapping table")
    m6_table = join_data(
        m6_table,
        mapping_table,
        "mostWatch",
        "keyword",
        select_cols
    )

    m7_table = join_data(
        m7_table,
        mapping_table,
        "mostWatch",
        "keyword",
        select_cols
    )

    select_cols2 = [col("a.user_id").alias("user_id"),
        col("a.mostWatch").alias("mostWatch_m6"),
        col("b.mostWatch").alias("mostWatch_m7"),
        col("a.category_std").alias("category_m6"),
        col("b.category_std").alias("category_m7")]
    logger.info("select_cols = %s", select_cols)


    logger.info("join June and July together and create `Customer Taste` columns")
    result = join_data(m6_table, m7_table, "user_id", "user_id", select_cols2)
    

    result = customertaste(result)
    result.show(10, truncate= False)

    # saving
    save_data(result, log_search + "final/")
    import_to_mysql(result, MYSQL_CONFIG)

if __name__ == "__main__":
    control_flow()