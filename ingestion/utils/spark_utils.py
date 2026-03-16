import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="GCP-Ingestion"):
    """
    Minimal Spark Session for Docker with GCS support.
    Increased memory limits to avoid OOM.
    """
    project_id = os.environ.get("GCP_PROJECT_ID", "project-f5ad855c-9811-45c4-a42")
    
    return (SparkSession.builder
        .appName(app_name)
        # Memory Management
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        # GCS Configuration & OOM Fix
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.gs.project.id", project_id)
        .config("spark.hadoop.google.cloud.auth.type", "COMPUTE_ENGINE_ACCOUNT")
        .config("spark.hadoop.fs.gs.outputformat.upload.chunk.size", "64m")
        .config("spark.hadoop.fs.gs.outputformat.upload.buffer.size", "32m")
        # Optimization
        .config("spark.sql.shuffle.partitions", "20")
        .getOrCreate())
