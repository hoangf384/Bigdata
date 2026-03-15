# outline

1. [Data Modeling](#1-Data-Modeling)

* 1.1 [Normal Forms (1NF → BCNF)](#11-Normal-Forms-1NF-→-BCNF)
* 1.2 [Dimensional modeling](#12-Dimensional-modeling)
* 1.3 [Star / Snowflake schema](#13-Star-Snowflake-schema)
* 1.4 [SCD (Type 1, 2, 3…)](#14-SCD-Type-1-2-3…)
* 1.5 [Fact vs Dimension](#15-Fact-vs-Dimension)
* 1.6 [Surrogate key vs natural key](#16-Surrogate-key-vs-natural-key)
* 1.7 [Data vault](#17-Data-vault)

2. [Database Internals](#2-Database-Internals)

* 2.1 [Index (B-tree, Hash)](#21-Index-B-tree-Hash)
* 2.2 [Partitioning](#22-Partitioning)
* 2.3 [MVCC](#23-MVCC)
* 2.4 [WAL](#24-WAL)
* 2.5 [Locking](#25-Locking)
* 2.6 [Isolation level](#26-Isolation-level)
* 2.7 [Query planner](#27-Query-planner)
* 2.8 [Execution plan](#28-Execution-plan)
* 2.9 [Cost-based optimizer](#29-Cost-based-optimizer)

3. [Storage Engine](#3-Storage-Engine)

* 3.1 [Row-based vs Columnar](#31-Row-based-vs-Columnar)
* 3.2 [Parquet / ORC](#32-Parquet-ORC)
* 3.3 [Compression](#33-Compression)
* 3.4 [Encoding (dictionary, RLE)](#34-Encoding-dictionary-RLE)
* 3.5 [Bloom filter](#35-Bloom-filter)
* 3.6 [Zone map](#36-Zone-map)

4. [Distributed Systems](#4-Distributed-Systems)

* 4.1 [Sharding](#41-Sharding)
* 4.2 [Replication](#42-Replication)
* 4.3 [CAP theorem](#43-CAP-theorem)
* 4.4 [Consensus (Raft)](#44-Consensus-Raft)
* 4.5 [Eventual consistency](#45-Eventual-consistency)
* 4.6 [Exactly-once semantics](#46-Exactly-once-semantics)
* 4.7 [Checkpointing](#47-Checkpointing)

5. [Data Architecture](#5-Data-Architecture)

* 5.1 [OLTP vs OLAP](#51-OLTP-vs-OLAP)
* 5.2 [Data Lake](#52-Data-Lake)
* 5.3 [Data Warehouse](#53-Data-Warehouse)
* 5.4 [Lakehouse](#54-Lakehouse)
* 5.5 [Medallion architecture](#55-Medallion-architecture)
* 5.6 [CDC](#56-CDC)
* 5.7 [Streaming vs Batch](#57-Streaming-vs-Batch)
* 5.8 [Lambda & Kappa architecture](#58-Lambda-Kappa-architecture)
* 5.9 [Message Queue](#59-Message-Queue)

6. [Performance Optimization](#6-Performance-Optimization)

* 6.1 [Partition pruning](#61-Partition-pruning)
* 6.2 [Clustering](#62-Clustering)
* 6.3 [Join strategies](#63-Join-strategies)
* 6.4 [Broadcast join](#64-Broadcast-join)
* 6.5 [Shuffle](#65-Shuffle)
* 6.6 [Skew handling](#66-Skew-handling)

7. [Reliability & Governance](#7-Reliability-Governance)

* 7.1 [Data quality](#71-Data-quality)
* 7.2 [Lineage](#72-Lineage)
* 7.3 [Schema evolution](#73-Schema-evolution)
* 7.4 [GDPR](#74-GDPR)
* 7.5 [Access control](#75-Access-control)
* 7.6 [Observability](#76-Observability)


## 1. Data Modeling

### 1.1 Normal Forms (1NF → 3NF)

Định nghĩa: Là tập hợp các quy tắc được chuẩn hóa trong quá trình thiết kế cơ sở dữ liệu quan hệ (RDBMS)

1NF: Mỗi thuộc tính chỉ chứa giá trị nguyên tử (atomic value)

2NF: 1NF + mọi thuộc tính non-key phải phụ thuộc hoàn toàn vào khóa chính (full functional dependency)

3NF: 2NF + mọi thuộc tính non-key không được phụ thuộc vào thuộc tính non-key khác (phụ thuộc bắc cầu)

### 1.2 Dimensional modeling

Định nghĩa: được thiết kế thành 2 phần, fact table và dimension table

fact table:
* chứa các con số định lượng, có thể tính toán được
* có nhiều loại fact table: transaction fact table, periodic snapshot fact table, accumulating snapshot fact table

dimension table: chứa các thông tin mô tả, ngữ cảnh của dữ liệu, trả lời các câu hỏi Who, What, Where, When, Why, How


### 1.3 Star / Snowflake schema

Star schema: là một dạng dimensional modeling, trong đó fact table nằm ở trung tâm và được bao quanh bởi các dimension table

> Cons/Pros: Query nhanh (ít join), nhưng dữ liệu bị dư thừa (denormalized)

Snowflake schema: là một dạng dimensional modeling, trong đó fact table nằm ở trung tâm và được bao quanh bởi các dimension table, và các dimension table này lại được bao quanh bởi các dimension table khác

> Cons/Pros: Không dư thừa dữ liệu (normalized), nhưng query phức tạp hơn (join nhiều)



### 1.4 SCD (Type 1, 2, 3…)

SCD: Được viết tắt bởi từ Slowly Changing Dimension, là một kỹ thuật để xử lý dữ liệu thay đổi theo thời gian.

type 1: Overwrite dòng cũ, pros: đơn giản, cons: mất lịch sử

Type 2: Thêm dòng mới, cách làm thì thêm flag **boolean**, pros: giữ lại toàn bộ lịch sử, cons: phức tạp hơn 1 xíu (5%) tốn dung lượng hơn. (giống versioning vậy) 
>  cột thêm thường là *is_current*, *ngày hiệu lực*, *ngày hết hiệu lực*.

Type 3: Thêm cột mới nhưng k versioning được pros: đơn giản hơn type 2, dữ liệu phình cố định, cons: mất lịch sử (chỉ lưu được 2 version)

### 1.5 Fact vs Dimension

Bảng Fact: chứa các con số định lượng, có thể tính toán được

Bảng Dimension: chứa các thông tin mô tả, ngữ cảnh của dữ liệu, trả lời các câu hỏi Who, What, Where, When, Why, How

### 1.6 Surrogate key vs natural key

Surrogate key: là một khóa do hệ thống (int auto increment, uuid), là duy nhất, không có ý nghĩa, dùng làm PK trong **dwh/olap**.
> vd: id: 1, 2, 3, UUID: 

Natural key: Do business định nghĩa (user_id, email, phone, account_number…), có ý nghĩa nghiệp vụ, thường dùng để identify entity trong OLTP, trong DWH: dùng để detect change [SCD](#14-SCD), không dùng làm PK.

### 1.7 Data vault

Định nghĩa: là một mô hình dữ liệu tương tự như star schema (kimball), Top-down (Bill Inmon) và Data vault (Dan Linstedt). ..

## 2. Database Internals

### 2.1 Index (B-tree, Hash)

Định nghĩa: là một kỹ thuật trong giải thuật, dùng để tăng tốc độ truy vấn dữ liệu.

Cons: 
* Tốn dung lượng khá nhiều, 30%-50% table, chỉ nên dùng cho các cột thường xuyên được truy vấn.
* tốn nhiều tài nguyên hơn khi ghi dữ liệu, ghi dữ liệu bị lâu 

Pros: Tăng tốc độ truy vấn dữ liệu lên rất cao vd: n = 10M -> Độ phức tạp là O(10M)
sau khi đánh index thì độ phức tạp giảm xuống còn O(log_b(n)) ~ 24 với b là số node trong cây index

### 2.2 Partitioning

Định nghĩa: là một kỹ thuật chia nhỏ dữ liệu ra (gần với nghĩa đen nhất)

Pros: 
* Tăng tốc độ truy vấn dữ rất nhiều
* Dễ dàng quản lý và bảo trì dữ liệu

Cons: 
* Data skew: là dữ liệu bị lệch về 1 phía. Vd: partition theo quốc gia, và đa số làm ở VN thì partition VN sẽ rất lớn, còn các partition khác sẽ rất nhỏ. Dẫn đến việc query dữ liệu bị chậm.
* Cross-partition join: là việc join dữ liệu giữa các partition, việc này sẽ tốn nhiều tài nguyên hơn.
* Bảo trì khó hơn vì phải liên tục đánh partition.

### 2.3 MVCC



### 2.4 WAL

### 2.5 Locking

### 2.6 Isolation level

### 2.7 Query planner

### 2.8 Execution plan

Định nghĩa: là plan để thực thi câu truy vấn, gồm các phần sau:

* Parsing – kiểm tra cú pháp
* Optimization – chọn execution plan
* Execution – chạy query theo plan

> **EXPLAIN (EXPLAIN ANALYZE)**: là lệnh dùng để xem stage *Optimization*

### 2.9 Cost-based optimizer


## 3. Storage Engine

### 3.1 Row-oriented vs Column-oriented

Định nghĩa: là 2 cách tổ chức lưu trữ dữ liệu trên disk.

Row-oriented (lưu theo hàng): mỗi hàng được lưu liền nhau trên disk. Phù hợp cho **OLTP** vì khi INSERT/UPDATE 1 dòng thì chỉ cần ghi 1 lần.
> Ví dụ: PostgreSQL, MySQL

Column-oriented (lưu theo cột): các giá trị của cùng 1 cột được lưu liền nhau trên disk. Phù hợp cho **OLAP** vì khi query chỉ cần đọc các cột cần thiết, bỏ qua các cột không liên quan → nhanh hơn rất nhiều khi query trên bảng rộng (nhiều cột).
> Ví dụ: BigQuery, Redshift, Parquet, ORC

| Tiêu chí | Row-based | Columnar |
|-----------|-----------|----------|
| Ghi (INSERT/UPDATE) | Nhanh | Chậm |
| Đọc (SELECT vài cột) | Chậm (đọc cả hàng) | Nhanh (chỉ đọc cột cần) |
| Nén dữ liệu | Kém (dữ liệu khác kiểu) | Tốt (dữ liệu cùng kiểu) |
| Use case | OLTP | OLAP, Data Lake |

### 3.2 Parquet / ORC

Định nghĩa: là 2 định dạng file **columnar** phổ biến nhất trong hệ sinh thái Big Data, dùng để lưu trữ dữ liệu trên Data Lake (S3, GCS, HDFS...).

**Parquet:**
* Được phát triển bởi Apache (Twitter + Cloudera)
* Hỗ trợ tốt với Spark, Hive, Presto, BigQuery
* Lưu schema trong file (self-describing)
* Hỗ trợ nested data (struct, array, map)

**ORC (Optimized Row Columnar):**
* Được phát triển bởi Hortonworks (nay là Cloudera)
* Tối ưu cho Hive
* Có built-in index và bloom filter
* Nén tốt hơn Parquet trong một số trường hợp

> Thực tế: Đa số các công ty hiện nay dùng **Parquet** vì tương thích rộng hơn (Spark, BigQuery, Snowflake đều đọc Parquet native).

### 3.3 Compression

Định nghĩa: là kỹ thuật nén dữ liệu để giảm dung lượng lưu trữ và tăng tốc I/O (đọc ít byte hơn từ disk).

Các codec phổ biến:

| Codec | Tỷ lệ nén | Tốc độ | Splittable | Use case |
|-------|-----------|--------|------------|----------|
| Snappy | Trung bình | Rất nhanh | Không | Spark default, query nhiều |
| GZIP | Cao | Chậm | Không | Lưu trữ lâu dài, ít query |
| ZSTD | Cao | Nhanh | Không | Cân bằng tốt giữa nén và tốc độ |
| LZ4 | Thấp | Cực nhanh | Không | Real-time, streaming |

> Lưu ý: Khi dùng Parquet, compression được áp dụng trên từng **column chunk**, nên hiệu quả nén rất cao vì dữ liệu cùng kiểu.

### 3.4 Encoding (dictionary, RLE)

Định nghĩa: là kỹ thuật mã hóa dữ liệu **trước khi nén**, giúp giảm kích thước dữ liệu bằng cách biểu diễn dữ liệu hiệu quả hơn.

**Dictionary Encoding:**


**RLE (Run-Length Encoding):**


### 3.5 Bloom filter

### 3.6 Zone map


## 4. Distributed Systems

### 4.1 Sharding

Định nghĩa: 

### 4.2 Replication

### 4.3 CAP theorem

Định nghĩa: là một định lý trong hệ thống phân tán, nói rằng trong 3 tính chất: Consistency (tính nhất quán), Availability (tính sẵn sàng), Partition tolerance (tính chịu lỗi phân vùng), thì chỉ có thể chọn 2 trong 3 tính chất.

> Vd: 


### 4.4 Consensus (Raft)

### 4.5 Eventual consistency

### 4.6 Exactly-once semantics

### 4.7 Checkpointing


## 5. Data Architecture

### 5.1 OLTP vs OLAP

OLTP (Online Transaction Processing): là kiến trúc thiết kế dùng dể tối ưu ghi, đọc dữ liệu liên tục, dùng thiết kế normal forms để xây dựng lên

OLAP (Online Analytical Processing): là kiến trúc thiết kế dùng dể tối ưu đọc dữ liệu, thường dùng để BI, reporting, dùng thiết kế dimensional modeling để xây dựng lên

| Tiêu chí | OLTP | OLAP |
|-----------|------|------|
| Mục đích | Ghi/đọc giao dịch | Phân tích, reporting |
| Thiết kế | Normal Forms (3NF) | Dimensional Modeling (Star/Snowflake) |
| Query | Ngắn, đơn giản (INSERT, UPDATE) | Dài, phức tạp (JOIN, GROUP BY, agg) |
| Dữ liệu | Mới nhất (current state) | Lịch sử (historical) |
| Ví dụ | PostgreSQL, MySQL | BigQuery, Redshift, Snowflake |

### 5.2 Data Lake

Định nghĩa: data lake là nơi lưu trữ dữ liệu **thô (raw)**, chấp nhận mọi định dạng: structured, semi-structured, unstructured. Lưu theo kiểu **schema-on-read** (đọc mới định nghĩa schema).

Đặc điểm:
* Lưu trữ **giá rẻ** (object storage)
* Hỗ trợ mọi loại file: CSV, JSON, Parquet, Avro, ảnh, video...
* Không tối ưu cho query trực tiếp (cần thêm engine như Spark, Presto)

> Ví dụ: S3 (AWS), GCS (Google), Azure Blob Storage, MinIO (Open Source)

### 5.3 Data Warehouse

Định nghĩa: data warehouse là nơi lưu trữ dữ liệu đã được **xử lý, chuẩn hóa (structured)**, tối ưu cho việc **query và phân tích**. Lưu theo kiểu **schema-on-write** (ghi phải tuân theo schema).

Đặc điểm:
* Query cực nhanh (columnar storage, indexing, caching)
* Dữ liệu phải qua ETL/ELT trước khi load vào
* Có query engine built-in, không cần thêm tool

> Ví dụ: Amazon Redshift, Google BigQuery, Snowflake, Azure Synapse Analytics

### 5.4 Lakehouse

Định nghĩa: data lakehouse là sự kết hợp giữa data lake và data warehouse, cung cấp khả năng lưu trữ dữ liệu linh hoạt của data lake và khả năng truy vấn, phân tích dữ liệu hiệu quả của data warehouse

Cách thực hiện: Dùng **table format** trên data lake để có tính năng của warehouse:
* **Delta Lake** (Databricks) — phổ biến nhất
* **Apache Iceberg** (Netflix) — đang phát triển nhanh
* **Apache Hudi** (Uber)

> Tính năng chính: ACID transactions, time travel, schema evolution — tất cả trên object storage (S3, GCS).

### 5.5 Medallion architecture

Định nghĩa: medallion architecture là một kiến trúc data lakehouse, được chia thành 3 tầng: bronze, silver, gold

| Tầng | Dữ liệu | Mô tả |
|------|---------|-------|
| **Bronze** | Raw | Dữ liệu thô, ingested nguyên bản từ source (JSON, CSV...) |
| **Silver** | Cleaned | Đã clean, deduplicate, chuẩn hóa kiểu dữ liệu, join enrichment |
| **Gold** | Business-ready | Đã aggregate, tạo metric, sẵn sàng cho BI/reporting |

> Flow: Source → **Bronze** (raw) → **Silver** (cleaned) → **Gold** (aggregated) → Dashboard

### 5.6 CDC

Định nghĩa: CDC (Change Data Capture) là một kỹ thuật để theo dõi và ghi lại các thay đổi trong dữ liệu, được sử dụng để đồng bộ dữ liệu giữa các hệ thống

Các phương pháp CDC:

| Phương pháp | Cách hoạt động | Pros/Cons |
|------------|---------------|-----------|
| **Log-based** | Đọc WAL/binlog của database | Realtime, không ảnh hưởng source. Phổ biến nhất |
| **Query-based** | Query dựa trên `updated_at` | Đơn giản nhưng miss delete, tốn tài nguyên source |
| **Trigger-based** | Database trigger ghi vào bảng audit | Chính xác nhưng ảnh hưởng hiệu năng source |

> Tool phổ biến: **Debezium** (open source, log-based), Fivetran, Airbyte

### 5.7 Streaming vs Batch

Định nghĩa: batch processing là xử lý dữ liệu theo lô, còn streaming là xử lý dữ liệu theo thời gian thực, tính bằng milisecond. Giữa việc chọn batch hay streaming là bài toán của doanh nghiệp, một số đặc tính của 2 loại này như là, batch xây dựng dễ hơn, bảo trì dễ hơn, cost ít hơn nhiều, còn streamming thì xây dựng rất khó và bảo trì cũng rất khó, nhưng một số đặc thù cần dữ liệu được xử lý ngay tức thì vd như hệ thống đặt xe uber, grab,...

| Tiêu chí | Batch | Streaming |
|-----------|-------|-----------|
| Độ trễ | Phút → giờ | Mili-giây → giây |
| Độ khó | Dễ xây, dễ bảo trì | Khó xây, khó debug |
| Chi phí | Thấp | Cao (luôn chạy 24/7) |
| Tool | Spark, Airflow, dbt | Kafka, Flink, Spark Streaming |
| Use case | Reporting hằng ngày, ETL | Fraud detection, ride-hailing, IoT |

### 5.8 Lambda & Kappa architecture

**Lambda Architecture:**
* Kết hợp cả batch layer và speed layer (streaming) chạy **song song**
* Batch layer: xử lý dữ liệu lớn, chính xác nhưng chậm (Spark batch)
* Speed layer: xử lý dữ liệu realtime, nhanh nhưng có thể chưa chính xác (Kafka + Flink)
* Serving layer: merge kết quả từ cả 2 layer để serve cho user
* **Cons**: phải maintain **2 pipeline** (batch + streaming) → code duplicate, khó bảo trì

**Kappa Architecture:**
* Chỉ dùng **1 streaming pipeline** cho tất cả (cả batch lẫn realtime)
* Coi batch như một trường hợp đặc biệt của streaming (replay lại event từ đầu)
* **Pros**: đơn giản hơn Lambda, chỉ 1 codebase
* **Cons**: phụ thuộc vào khả năng replay của message queue (Kafka retention)

> Thực tế: Đa số công ty hiện nay chọn **Kappa** hoặc hybrid, vì streaming framework (Flink, Spark Structured Streaming) đã đủ mạnh để xử lý cả batch.

### 5.9 Message Queue

Định nghĩa: là hệ thống trung gian (**middleware**) dùng để truyền message giữa các service/hệ thống một cách **bất đồng bộ (async)**, đảm bảo producer và consumer không cần biết nhau.

Tại sao cần Message Queue:
* **Decoupling**: Producer và consumer hoạt động độc lập
* **Buffer**: Khi consumer chậm hơn producer → message queue giữ lại, không mất data
* **Scalability**: Thêm consumer để xử lý song song

Các loại phổ biến:

| Tool | Đặc điểm | Use case |
|------|---------|----------|
| **Apache Kafka** | Distributed, durable, replay được, throughput cực cao | Event streaming, CDC, log aggregation |
| **RabbitMQ** | Lightweight, routing linh hoạt, acknowledge message | Task queue, microservice communication |
| **AWS SQS** | Fully managed, serverless | Simple queue trên AWS |
| **Google Pub/Sub** | Fully managed, global | Streaming pipeline trên GCP |

> Trong Data Engineering, **Kafka** là lựa chọn phổ biến nhất vì hỗ trợ replay, retention, và tích hợp tốt với Spark/Flink.


## 6. Performance Optimization

### 6.1 Partition pruning

Định nghĩa: là kỹ thuật **bỏ qua (skip)** các partition không liên quan khi thực thi query, nhờ vào điều kiện WHERE.

Cách hoạt động:
* Bảng được partition theo cột (vd: `date`, `country`)
* Khi query có `WHERE date = '2025-01-01'`, engine chỉ đọc partition `date=2025-01-01`, bỏ qua tất cả partition khác
* Giảm lượng dữ liệu cần scan từ **TB xuống GB**

> Ví dụ thực tế: BigQuery partition theo `_PARTITIONDATE`, Hive partition theo thư mục `year=2025/month=01/day=01/`

Lưu ý: Partition pruning chỉ hoạt động khi điều kiện WHERE **trùng với partition key**. Nếu query không filter theo partition key → full scan.

### 6.2 Clustering

### 6.3 Join strategies

Định nghĩa: là các chiến lược mà engine (Spark, Presto, BigQuery...) sử dụng để thực hiện phép JOIN giữa 2 bảng.

Các loại chính:

| Strategy | Cách hoạt động | Khi nào dùng |
|----------|---------------|--------------|
| **Nested Loop Join** | So sánh từng dòng bảng A với từng dòng bảng B | Bảng nhỏ, không có join key |
| **Hash Join** | Build hash table từ bảng nhỏ, probe bằng bảng lớn | Equi-join (`=`), phổ biến nhất |
| **Sort-Merge Join** | Sort cả 2 bảng theo join key, rồi merge | Dữ liệu đã sort sẵn, range join |
| **Broadcast Join** | Gửi bảng nhỏ đến tất cả node | Xem 6.4 |

### 6.4 Broadcast join

Định nghĩa: là chiến lược join trong hệ thống phân tán, trong đó **bảng nhỏ được copy (broadcast) đến tất cả các node** chứa bảng lớn, thay vì shuffle bảng lớn.

Cách hoạt động:
* Bảng nhỏ (vd: dim_country, 200 dòng) được gửi đến **mọi executor/worker**
* Mỗi executor join bảng lớn local của nó với bảng nhỏ → **không cần shuffle**
* Nhanh hơn rất nhiều so với shuffle join

Điều kiện:
* Bảng nhỏ phải **đủ nhỏ** để fit vào RAM của mỗi executor
* Spark default: broadcast bảng < **10MB** (`spark.sql.autoBroadcastJoinThreshold`)

> Lưu ý: Nếu broadcast bảng quá lớn → OOM (Out Of Memory). Cần kiểm tra size trước.

### 6.5 Shuffle

Định nghĩa: là quá trình **tái phân phối (redistribute)** dữ liệu giữa các node/executor trong hệ thống phân tán, thường xảy ra khi thực hiện các phép JOIN, GROUP BY, DISTINCT.

Tại sao shuffle tốn kém:
* Dữ liệu phải đi qua **network** (I/O chậm nhất)
* Phải **serialize → gửi → deserialize** dữ liệu
* Ghi dữ liệu trung gian ra **disk** (spill)
* Là nguyên nhân #1 khiến Spark job chậm

Cách giảm shuffle:
* Dùng **broadcast join** thay vì shuffle join (khi 1 bảng nhỏ)
* **Pre-partition** dữ liệu theo join key (co-partition)
* Dùng **bucket** trong Hive/Spark để dữ liệu đã được hash sẵn
* Tăng `spark.sql.shuffle.partitions` (default 200) nếu data lớn

### 6.6 Skew handling

Định nghĩa: Data skew là hiện tượng dữ liệu **phân bố không đều** giữa các partition/task, khiến 1 task xử lý nhiều hơn hẳn các task khác → **bottleneck**, job chạy chậm.

Ví dụ thực tế:
* JOIN theo `country_code` → partition "VN" có 10M dòng, partition "LA" chỉ có 1K dòng
* GROUP BY `user_id` → 1 user có 5M events, các user khác chỉ 100 events

Cách xử lý:

| Kỹ thuật | Mô tả |
|----------|-------|
| **Salting** | Thêm random prefix vào key bị skew → chia đều thành nhiều partition nhỏ → aggregate 2 lần |
| **Broadcast join** | Nếu 1 bảng nhỏ → broadcast thay vì shuffle |
| **Adaptive Query Execution (AQE)** | Spark 3.0+ tự detect skew và split partition lớn tại runtime |
| **Isolate skew key** | Tách riêng key bị skew ra xử lý riêng, rồi UNION lại |

> Tip: Luôn kiểm tra skew bằng cách xem **Spark UI → Stage → Task Duration**. Nếu 1 task chạy lâu hơn hẳn → có skew.


## 7. Reliability & Governance

### 7.1 Data quality

### 7.2 Lineage

### 7.3 Schema evolution

### 7.4 GDPR

### 7.5 Access control

### 7.6 Observability
