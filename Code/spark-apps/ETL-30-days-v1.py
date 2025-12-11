from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, when, col

spark = SparkSession.builder \
    .appName("ETL") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()


folder_path = "/data/raw/log_content/20220401.json" 
save_path = "/data/destination/20220401"


def read_data(path):
    df = spark.read.json(path)
    return df

def save_data(result, path):
    (
        result
        .repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    print("Data Saved Successfully")

def select_fields(df):
    df = df.select("_source.*")
    return df

def calculate_devices(df):
    return df.select("Contract","Mac") \
        .groupBy("Contract") \
        .agg(count("Mac").alias("Device_Count")) 

def transform_category(df):
    return df.withColumn(
        "Type",
        when(
            col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"),
            "Truyền Hình"
        )
        .when(
            col("AppName").isin("VOD", "FIMS_RES", "BHD_RES", "VOD_RES", "FIMS", "BHD", "DANET"),
            "Phim Truyện"
        )
        .when(col("AppName") == "RELAX", "Giải Trí")
        .when(col("AppName") == "CHILD", "Thiếu Nhi")
        .when(col("AppName") == "SPORT", "Thể Thao")
        .otherwise("Error")
    )

def calculate_statistics(df):
    return (
        df.groupBy("Contract", "Type")
          .agg(sum("TotalDuration").alias("TotalDuration"))
          .groupBy("Contract")
          .pivot("Type")
          .sum("TotalDuration")
          .na.fill(0)
    )

def finalize_result(statistics,total_devices):
    result = statistics.join(total_devices,'Contract','inner')
    return result 

def main(path):
    print('-------------Reading data from path--------------')
    df = read_data(path)
    df.show()
    print('-------------Selecting fields--------------')
    df = select_fields(df)
    df.show()
    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)
    total_devices.show()
    print('-------------Transforming Category --------------')
    df = transform_category(df)
    df.show()
    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)
    statistics.show()
    print('-------------Finalizing result --------------')
    result = finalize_result(statistics, total_devices)
    result.show()
    print('-------------Saving Results --------------')
    save_data(result, save_path)
    return print('Task finished')
    
main(folder_path)