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

### 1.1 Normal Forms (1NF → BCNF)

Định nghĩa:

1NF: Mỗi thuộc tính chỉ chứa giá trị nguyên tử (atomic value)

2NF: 1NF + mọi thuộc tính non-key phải phụ thuộc hoàn toàn vào khóa chính (full functional dependency)

3NF: 2NF + mọi thuộc tính non-key không được phụ thuộc vào thuộc tính non-key khác (phụ thuộc lẫn nhau)

BCNF: 

### 1.2 Dimensional modeling

Định nghĩa: được thiết kế thành 2 phần, fact table và dimension table

fact table:
* chứa các con số định lượng, có thể tính toán được
* có nhiều loại fact table: transaction fact table, periodic snapshot fact table, accumulating snapshot fact table

dimension table: chứa các thông tin mô tả, ngữ cảnh của dữ liệu, trả lời các câu hỏi Who, What, Where, When, Why, How


### 1.3 Star / Snowflake schema

Star schema: là một dạng dimensional modeling, trong đó fact table nằm ở trung tâm và được bao quanh bởi các dimension table

Snowflake schema: là một dạng dimensional modeling, trong đó fact table nằm ở trung tâm và được bao quanh bởi các dimension table, và các dimension table này lại được bao quanh bởi các dimension table khác

### 1.4 SCD (Type 1, 2, 3…)

SCD: Được viết tắt bởi từ Slowly Changing Dimension, là một kỹ thuật để xử lý dữ liệu thay đổi theo thời gian.

type 1: Overwrite dòng cũ, pros: đơn giản, cons: mất lịch sử

Type 2: Thêm dòng mới, cách làm thì thêm flag **boolean**, pros: giữ lại toàn bộ lịch sử, cons: phức tạp hơn 1 xíu (5%) tốn dung lượng hơn. (giống versioning vậy) 

Type 3: Thêm cột mới nhưng k versioning được pros: đơn giản hơn type 2, dữ liệu phình cố định, cons: mất lịch sử (chỉ lưu được 2 version)

### 1.5 Fact vs Dimension

Bảng Fact: chứa các con số định lượng, có thể tính toán được

Bảng Dimension: chứa các thông tin mô tả, ngữ cảnh của dữ liệu, trả lời các câu hỏi Who, What, Where, When, Why, How

### 1.6 Surrogate key vs natural key

Surrogate key: là một khóa do hệ thống (int auto increment, uuid), là duy nhất, không có ý nghĩa, dùng làm PK trong **dwh/olap**.
> vd: id: 1, 2, 3, UUID: 

Natural key: Do business định nghĩa (user_id, email, phone, account_number…), có ý nghĩa nghiệp vụ, thường dùng để identify entity trong OLTP, trong DWH: dùng để detect change [SCD](#14-SCD), không dùng làm PK.

### 1.7 Data vault


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

### 2.3 MVCC

### 2.4 WAL

### 2.5 Locking

### 2.6 Isolation level

### 2.7 Query planner

### 2.8 Execution plan

### 2.9 Cost-based optimizer


## 3. Storage Engine

### 3.1 Row-based vs Columnar

### 3.2 Parquet / ORC

### 3.3 Compression

### 3.4 Encoding (dictionary, RLE)

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

### 5.2 Data Lake

Định nghĩa: data lake là nơi tối ưu để lưu trữ dữ liệu, một số datalake tiêu biểu là S3, GCS, Azure Blob Storage, MinIO (Open Source Framework)

### 5.3 Data Warehouse

Định nghĩa: data warehouse là nơi tối ưu để lưu trữ dữ liệu, một số data warehouse tiêu biểu là Amazon Redshift, Google BigQuery, Snowflake, Microsoft Azure Synapse Analytics

### 5.4 Lakehouse

Định nghĩa: data lakehouse là sự kết hợp giữa data lake và data warehouse, cung cấp khả năng lưu trữ dữ liệu linh hoạt của data lake và khả năng truy vấn, phân tích dữ liệu hiệu quả của data warehouse

### 5.5 Medallion architecture

Định nghĩa: medallion architecture là một kiến trúc data lakehouse, được chia thành 3 tầng: bronze, silver, gold

### 5.6 CDC

Định nghĩa: CDC là một kỹ thuật để theo dõi và ghi lại các thay đổi trong dữ liệu, được sử dụng để đồng bộ dữ liệu giữa các hệ thống

### 5.7 Streaming vs Batch

Định nghĩa: batch processing là xử lý dữ liệu theo lô, còn streaming là xử lý dữ liệu theo thời gian thực, tính bằng milisecond. Giữa việc chọn batch hay streaming là bài toán của doanh nghiệp, một số đặc tính của 2 loại này như là, batch xây dựng dễ hơn, bảo trì dễ hơn, cost ít hơn nhiều, còn streamming thì xây dựng rất khó và bảo trì cũng rất khó, nhưng một số đặc thù cần dữ liệu được xử lý ngay tức thì vd như hệ thống đặt xe uber, grab,...

### 5.8 Lambda & Kappa architecture

### 5.9 Message Queue



## 6. Performance Optimization

### 6.1 Partition pruning

### 6.2 Clustering

### 6.3 Join strategies

### 6.4 Broadcast join

### 6.5 Shuffle

### 6.6 Skew handling


## 7. Reliability & Governance

### 7.1 Data quality

### 7.2 Lineage

### 7.3 Schema evolution

### 7.4 GDPR

### 7.5 Access control

### 7.6 Observability
