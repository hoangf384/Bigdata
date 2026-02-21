# Big Data Project - OTT Log Analytics Pipeline

Xem hướng dẫn thực thi local tại [README.md](local/README.md)

---

## Lộ trình Chuyển đổi GCP MVP (Serverless)


### 1. Kiến trúc chuyển đổi Tech Stack sang GCP
| Thành phần | Local/Docker | **GCP MVP (Serverless)** |
| :--- | :--- | :--- |
| **Storage (Data Lake)** | Local Disk | **Google Cloud Storage (GCS)** |
| **Data Warehouse & Processing** | Spark on Docker | **BigQuery (SQL-based)** |
| **Serving Layer** | MySQL (Docker) | **BigQuery** |
| **Visualization (BI)** | Metabase (Local) | **Looker Studio** |
| **Orchestration** | Manual / Scripts | **BigQuery Jobs / Scheduled Queries** |

---

### 2. Checklist Thực hiện MVP

#### Giai đoạn 1: Chuẩn bị Data Lake (GCS) 
*   [x] **Tạo Bucket:** Đã tạo bucket `bigdata-proj` tại Singapore (`asia-southeast1`).
*   [x] **Phân cấp Folder:** Tạo folder `raw/` để chứa các file thô, `final/` để chứa file đã xử lý.

> Đã hoàn thành

#### Giai đoạn 2: Kho dữ liệu & Xử lý (BigQuery)

*   [ ] **Tạo Dataset:** Tạo dataset `ott_analysis` tại Singapore.
*   [ ] **Định nghĩa Schema:** Tạo bảng bằng tính năng "Auto-detect schema" khi load file từ GCS.
*   [ ] **Load Data:** Thiết lập job load dữ liệu (qua `bq load` hoặc giao diện console).
*   [ ] **SQL Transformation:** Chuyển đổi logic từ PySpark sang SQL (View/Table) trực tiếp trong BigQuery.

#### Giai đoạn 3: Trực quan hóa (Looker Studio)
*   [ ] **Kết nối nguồn dữ liệu:** Kết nối Looker Studio với BigQuery.
*   [ ] **Chọn bảng:** Trỏ tới đúng Project -> Dataset -> Table/View.
*   [ ] **Thiết kế Dashboard:** Kéo thả các biểu đồ phân tích thời lượng, hành vi người dùng.

#### Giai đoạn 4: Quản lý chi phí & Tự động hóa
*   [ ] **Budget Alert:** Cài đặt cảnh báo để kiểm soát 7.8M credit.
*   [ ] **Tối ưu hóa:** Sử dụng mô hình chi phí "On-demand".

### 3. Hình ảnh minh họa

```mermaid
graph LR
    subgraph "Google Cloud Platform <br/>(MVP)"
        A[(Cloud Storage<br/>Raw Data Zone)]
        B[(BigQuery<br/>Data Warehouse)]
        C[Looker Studio<br/>Dashboards]
    end

    A -->|Upload file JSON/Parquet| B
    B -->|Load Data / External Table| C
```