import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import ETL_1_day as etl

# Spark session
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

# Paths
folder_path = "/data/raw/log_content/"
save_path = "/data/destination/log_content/"

# List files
def list_files_sorted(path):
    files = [
        os.path.join(path, f)
        for f in os.listdir(path)
        if f.endswith(".json") and os.path.isfile(os.path.join(path, f))
    ]
    return sorted(files)

def extract_date_from_filename(path):
    """
    /data/raw/log_content/20220401.json → 20220401
    """
    base = os.path.basename(path)      # 20220401.json
    date = base.replace(".json", "")   # 20220401
    return date

def main():
    all_files = list_files_sorted(folder_path)
    print("Files to process:", all_files)

    final_df = None

    for file in all_files:
        print(f"Processing file: {file}")

        # Extract date
        date_str = extract_date_from_filename(file)

        # 1 day ETL
        df = etl.read_data(spark, file)
        df = etl.select_fields(df)
        df = etl.transform_category(df)
        df = df.filter(df.Type != "Error")

        # summarize & pivot
        day_result = etl.pivot_table(df)

        # add date column
        day_result = day_result.withColumn("Date", lit(date_str))

        # union all days
        if final_df is None:
            final_df = day_result
        else:
            final_df = final_df.unionByName(day_result)

    print("Saving final output...")
    etl.save_data(final_df, save_path)

    print("---- ETL 30 DAYS COMPLETED ----")


main()