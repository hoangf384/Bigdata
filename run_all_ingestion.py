import subprocess
import time
import os
import sys

def find_spark_home():
    """Tự động tìm kiếm SPARK_HOME và các thư mục python liên quan."""
    spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
    python_path = os.path.join(spark_home, "python")
    lib_path = os.path.join(python_path, "lib")
    
    py4j_zip = None
    if os.path.exists(lib_path):
        for file in os.listdir(lib_path):
            if file.startswith("py4j-") and file.endswith(".zip"):
                py4j_zip = os.path.join(lib_path, file)
                break
                
    return python_path, py4j_zip

# Danh sách các Job (Đường dẫn bên trong container)
INGESTION_JOBS = [
    "/ingestion/jobs/ingest_user_registrations.py",
    "/ingestion/jobs/ingest_contracts.py",
    "/ingestion/jobs/ingest_keyword_mapping.py",
    "/ingestion/jobs/ingest_viewership_months.py",
    "/ingestion/jobs/ingest_log_content.py",
    "/ingestion/jobs/ingest_log_search.py"
]

def run_job(job_path):
    """Thực thi job PySpark với PYTHONPATH được tìm kiếm tự động."""
    print(f"\n>>> [EXECUTING] {job_path}...")
    start_time = time.time()
    
    spark_python, py4j_zip = find_spark_home()
    
    env = os.environ.copy()
    # Kết hợp các đường dẫn tìm được
    paths = [spark_python, "/app"]
    if py4j_zip:
        paths.append(py4j_zip)
    
    env["PYTHONPATH"] = ":".join(paths) + ":" + env.get("PYTHONPATH", "")
    env["SPARK_HOME"] = os.environ.get("SPARK_HOME", "/opt/spark")
    
    try:
        # Sử dụng spark-submit thay vì python3 để đảm bảo Driver có đủ RAM 4GB ngay khi khởi chạy
        spark_submit_path = os.path.join(os.environ.get("SPARK_HOME", "/opt/spark"), "bin/spark-submit")
        
        command = [
            spark_submit_path,
            "--driver-memory", "4g",
            "--executor-memory", "4g",
            job_path
        ]
        
        subprocess.run(command, env=env, check=True)
        duration = time.time() - start_time
        print(f">>> [SUCCESS] {job_path} (Took {duration:.2f}s)")
        return True, duration
    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f">>> [FAILED] {job_path} (Exit Code: {e.returncode})")
        return False, duration

def main():
    print("="*60)
    print("🐳 BRONZE INGESTION PIPELINE (AUTO-FIND SPARK)")
    print("="*60)
    
    overall_start = time.time()
    summary = []
    
    for job in INGESTION_JOBS:
        success, duration = run_job(job)
        summary.append({
            "job": job,
            "status": "✅ SUCCESS" if success else "❌ FAILED",
            "duration": f"{duration:.2f}s"
        })
        
    print("\n" + "="*60)
    print("📊 INGESTION SUMMARY REPORT")
    print("-"*60)
    for item in summary:
        print(f"{item['status']} | {item['job']:<45} | {item['duration']}")
    
    total_duration = time.time() - overall_start
    print("-"*60)
    print(f"Total Time: {total_duration/60:.2f} minutes")
    print("="*60)

if __name__ == "__main__":
    main()
