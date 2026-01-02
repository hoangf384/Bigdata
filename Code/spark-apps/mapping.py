from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
import sys

# -----------------------------------
# init
# -----------------------------------

spark = (
        SparkSession.builder
        .appName("Map-Category")
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("ERROR")

input_path = "/data/destination/log_search/category/mapping.jsonl"
output_path = "/data/destination/log_search/category/mapping_std"


# -----------------------------------
# Category mapping logic
# -----------------------------------


def map_category(df, src_col="category", dst_col="category_std"):
    """
    Chuẩn hoá category về tập category chuẩn.
    """

    return (
        df.withColumn(
            dst_col,
            when(col(src_col).isin("Action"), "Action")
            .when(col(src_col).isin("Romance"), "Romance")
            .when(col(src_col).isin("Comedy"), "Comedy")
            .when(col(src_col).isin("Horror"), "Horror")
            .when(col(src_col).isin("Animation", ".Animation", "Animation/Drama"), "Animation")
            .when(col(src_col).isin("Drama", "Drama/Other"), "Drama")
            .when(col(src_col).isin("C Drama"), "C Drama")
            .when(col(src_col).isin("K Drama", "KDrama"), "K Drama")
            .when(col(src_col).isin("Sports", "Sport", "Live Sports", "Sports/Drama"), "Sports")
            .when(col(src_col).isin("Music", "Music/Drama"), "Music")
            .when(
                col(src_col).isin(
                    "Reality Show",
                    "Reality",
                    "Dance/Reality",
                    "Cooking/Reality"
                ),
                "Reality Show"
            )
            .when(col(src_col).isin("TV Channel", "Live"), "TV Channel")
            .when(col(src_col).isin("News"), "News")
            .otherwise("Other")
        )
    )

def save_data(df):
    return (
        df
        .repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .json(output_path)
    )

# -----------------------------------
# Main
# -----------------------------------

def main():
    # -------------------------------
    # Read
    # -------------------------------
    
    df = spark.read.json(input_path)

    print("Raw category distribution:")
    df.groupBy("category") \
      .count() \
      .orderBy(F.desc("count")) \
      .show(87, truncate=False)

    # -------------------------------
    # Transform
    # -------------------------------
    df = map_category(df, src_col="category", dst_col="category_std")

    print("Standardized category distribution:")
    df.groupBy("category_std") \
      .count() \
      .orderBy(F.desc("count")) \
      .show(87, truncate=False)

    # keep needed columns only
    df = df.select("keyword", "category_std")

    # -------------------------------
    # Save
    # -------------------------------

    save_data(df)

    print(f"Mapping saved to {output_path}")


if __name__ == "__main__":
    main()