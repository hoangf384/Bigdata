# data storage architecture & execution methods
## mục lục
0. [viết tắt](#0-viết-tắt)
1. [storage layer](#1-storage-layer)
    
    - [storage architecture](#11-storage-architecture)
        
        - [data lake structure](#111-data-lake-strucvture)
        
        - [data warehouse structure](#112-data-warehouse-structure)
        
        - [data lakehouse structure](#113-data-lakehouse-structure)
        
        - [ODS](#14-ods)
        
        - [vector DB](#15-vector-db)

2. [execution methods layer](#2-execution-methods-layer)
    
    - [sql engine](#21-sql-engine)
    
    - [nosql engine](#22-nosql-engine)
    
    - [dataframe engine](#23-dataframe-engine)
    
    - [emmbedding engine](#24-emmbedding-engine)

    - [vector search engine](#25-vector-search-engine)

3. [processing paradigm](#3-processing-paradigm)

    - [batch transform](#31-batch-transform)
    
    - [batching transform](#32-streaming-transform)

[MISC](#misc)

## 0. Viết tắt

- DL: data lake
- DLH: data lakehouse
- DW: data warehouse
- OSF: object source framework

## 1. storage layer

Phần này giống như introduction về mọi thứ khởi nguyên trong lưu trữ dữ liệu

### 1.1 data lake 

> data lake = object storage / file system \
> object storage là hệ thống lưu trữ dữ liệu dưới dạng object, mỗi object bao gồm dữ liệu, metadata và unique identifier \
> object storage thường được sử dụng để lưu trữ dữ liệu phi cấu trúc (unstructured data) như file ảnh, video, log file v.v... \
> object storage phổ biến hiện nay có: Amazon S3, Google Cloud Storage, Azure Blob Storage, MinIO (Open Source Framework) v.v \
> google drive cũng là object storage (ở góc nhìn nào đó)

### 1.2 data warehouse

> data warehouse là hệ thống dữ liệu được thiết kế để hỗ trợ việc phân tích và báo cáo dữ liệu \
> data warehouse thường sử dụng mô hình quan hệ (relational model) để tổ chức dữ liệu, với các bảng (tables) được liên kết với nhau thông qua các khóa (keys) \
> data warehouse thường sử dụng các công cụ ETL (Extract, Transform, Load) để chuyển đổi và tải dữ liệu từ các nguồn khác nhau vào kho dữ liệu \
> data warehouse phổ biến hiện nay có: Amazon Redshift, Google BigQuery, Snowflake, Microsoft Azure Synapse Analytics v.v

### MISC
#### SCD (slowly changing dimension)
- SCD type 1: overwrite
- SCD type 2: add new row
- SCD type 3: add new column

### 1.3 data lakehouse

> data lakehouse là sự kết hợp giữa data lake và data warehouse, cung cấp khả năng lưu trữ dữ liệu linh hoạt của data lake và khả năng truy vấn, phân tích dữ liệu hiệu quả của data warehouse \
> data lakehouse thường sử dụng object storage để lưu trữ dữ liệu, table format để tổ chức và quản lý dữ liệu, và compute layer để xử lý và phân tích dữ liệu

| layer         | tools                 |
|---|---|
| storage layer | (minio)               |
| table format  | (iceberg/delta lake)  |
| compute layer | (trino/spark)         |


### 1.4 ODS

Sẽ viết sau

### 1.5 vector DB

Sẽ viết sau

---
### MISC
#### table format

- là tập hợp các quy tắc, định dạng để tổ chức và quản lý dữ liệu trong data lake 
- cung cấp các tính năng như: schema enforcement, ACID transaction, versioning, time travel v.v... 
- giúp biến data lake thành data lakehouse, hỗ trợ các công cụ compute layer như Spark, Trino, Presto v.v... để truy vấn và phân tích dữ liệu một cách hiệu quả hơn
 
#### medallion architecture
bronze level: sao chép dữ liệu lại từ data source, không transform, apply table format (iceberg, delta lake) \
silver level: tương đương với transformating lavel, chỉ transform nhẹ, ràng buộc điều kiện \
gold level: tầng cao nhất, feed được dữ liệu cho ML, BI tools v.v... 

#### different type of loading methods
- batch processing
    - full load (lần đầu)
    - incremental load (những lần sau)
- stream processing
    - micro-batch
    - real-time stream processing

## 2. execution methods layer

excution methods layer là lớp xử lý dữ liệu, trong đó có 2 engine lớn nhất là sql engine và NoSQL engine, dataframe engine cũng khá nổi, như là Pandas, Polars, Dask, tuy nhiên Pandas performace quá yếu để gọi là DF engine được, thay vào đó dùng spark tốt hơn nhiều.

### 2.1 SQL engine

- trino/presto (trino là bản nâng cấp của presto)
- Spark
- Hive

### 2.2 NoSQL engine

- Cassandra
- mongoDB
- hbase

### 2.3 Dataframe engine

- Polars
- Pyspark.dataframe
- Dask

### 2.4 Emmbedding engine

### 2.5 Vector search engine

--- 
tùy mục đích sẽ chọn các engine khác nhau để xử lý dữ liệu, ví dụ:

1. Nhánh DW: trong trường hợp muốn xuất dữ liệu để phân tích (analytic serving layer) thì sẽ chọn sql engine (heavy transform operation, ETL), xuất ra semi-structured data / structured data
2. Nhánh DLH: cùnng với SQL engine, nhưng kết hợp thêm table format, DL sẽ biến thành DLH
3. Nhánh ODS: trong trường hợp muốn lưu trữ dữ liệu metadata thì sẽ sử dụng NoSQL engine (light transform operation)
4. Nhánh vector DB: Nhánh này khá mới, nổi những năm gần đây, sử dụng LLM để trong execution engine, mình chưa tìm hiểu kỹ nên chỉ viết ngang đây thôi

> hiểu đơn giản là: storage layer + execution layer (light/heavy transform) + MISC = data platform
## 3. processing paradigm

Hai cơ chế chính trong flow dữ liệu là batch và streaming processing

### 3.1 batch transform 

Sẽ viết sau

### 3.2 Stream transform

Sẽ viết sau

## MISC
> duckDB hiểu đơn giản là execution engine (SQL engine), query trực tiếp flat file (csv, parquet, json...) mà k cần load vào dbms (DuckDB = execution engine(SQL) + embeddingDB (sqllite-like)) \
> Polars cũng là execution engine (dataframe engine) xử lý dataframe bằng python, dễ hiểu hơn là Pandas + Arrow = Polars 

[Apache Arrow](https://arrow.apache.org/docs/format/Intro.html) \
[Polars](https://docs.pola.rs/) \
[DuckDB](https://duckdb.org/docs/introduction)