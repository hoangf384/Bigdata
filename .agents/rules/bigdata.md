---
trigger: always_on
---

    1 # Gemini CLI Project Mandates - Bigdata Project
    2
    3 ## 1. Ngữ cảnh Công nghệ (Tech Stack)
    4 - **Data Warehouse:** Sử dụng DuckDB (`local/duckdb/myduckdb.duckdb`).
    5 - **Transformation:** Sử dụng dbt (nằm trong `local/dbt`).
    6 - **ETL/Processing:** Sử dụng PySpark và Python thuần (nằm trong `local/pipelines`).
    7 - **Infrastructure:** Quản lý qua Docker Compose (trong `local/infra`).
    8 - **Environment:** Luôn ưu tiên sử dụng virtual environment tại `/home/hp/Bigdata/.venv`.
    9
   10 ## 2. Tiêu chuẩn dbt
   11 - Khi thay đổi bất kỳ model nào trong `local/dbt/models`, phải kiểm tra file schema tương ứng (`_sources.yml`, `_core_models.yml`, v.v.).
   12 - Sau khi sửa code SQL của dbt, luôn chạy `dbt run` và `dbt test` cho model đó để xác thực.
   13 - SQL định dạng theo kiểu: Keyword viết HOA, sử dụng CTE thay vì subqueries phức tạp.
   14
   15 ## 3. Tiêu chuẩn Python & Spark
   16 - Code Python tuân thủ PEP 8.
   17 - Khi làm việc với Spark, luôn ưu tiên kiểm tra cấu hình trong `local/infra/spark/Dockerfile` hoặc `docker-compose.yaml` để hiểu môi trường chạy.
   18 - Luôn kiểm tra log hoặc output trong `local/data/destination` sau khi chạy script ETL.
   19
   20 ## 4. Quản lý Dữ liệu & Bảo mật
   21 - **TUYỆT ĐỐI KHÔNG** thay đổi hoặc xóa dữ liệu trong `local/data/raw` trừ khi có yêu cầu cụ thể.
   22 - Không bao giờ commit các file `.env` hoặc file dữ liệu lớn (`.csv`, `.parquet`, `.duckdb`) lên Git.
   23 - Khi cần tạo dữ liệu mẫu, hãy tham khảo `local/gendata/script.py`.
   24
   25 ## 5. Quy trình làm việc (Workflow)
   26 - **Nghiên cứu:** Luôn kiểm tra `README.md` trong các thư mục con để hiểu mục đích của module trước khi sửa.
   27 - **Xác thực:** Mọi thay đổi về code xử lý dữ liệu (SQL/Python) phải được đi kèm với bước chạy thử (run) và kiểm tra kết quả (validate).