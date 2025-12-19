# Big data tutorial 
## Tổng quan:

> Hướng dẫn Big data từ A-Z, bao gồm các công cụ phổ biến và miễn trong Big data hiện nay như: Spark, Kafka, Cassandra, Superset, Airflow, Docker... \
**Lưu ý:** \
> Tất cả những gì mình làm đều trên máy tính cá nhân, cloud mình chưa dùng tới (có thể sẽ sử dụng sau) 

## Mục lục
1. [Tổng quan](#tổng-quan)
2. [Kiến trúc](#kiến-trúc)
3. [Công cụ](#công-cụ)
4. [Luồng dữ liệu di chuyển (data linege flow)](#uồng-dữ-liệu-di-chuyển-data-linege-flow)
   - [giai đoạn 1: kiểm thử dữ liệu (data testing)](#giai-đoạn-1-kiểm-thử-dữ-liệu-data-testing)
   - [giai đoạn 2: xử lý dữ liệu (data processing)](#giai-đoạn-2-xử-lý-dữ-liệu-data-processing)
5. [ETL Logic](#etl-logic)
6. [Schema](#schema)
7. [Run Instructions](#run-instructions)
8. [Folder Structure](#folder-structure)
9. [Author / Version](#author--version)
## Kiến trúc
### Công Cụ 

| Category                  |   :   |Items                                          |
|---------------------------|-------|-----------------------------------------------|
| Core Big Data Processing  |   :   | Apache Spark, Apache Kafka, Apache Airflow    |
| Storage Layer             |   :   | Cassandra, MySQL                              |
| Analytics & Visualization |   :   | Apache Superset                               |
| Development & Prototyping |   :   | Jupyter Notebook, Python, Scala               |
| Infrastructure / DevOps   |   :   | Docker, Docker Compose, Linux (Fedora)        |
| Source Control            |   :   | Git, GitHub                                   |
| IDEs                      |   :   | VS Code, DataGrip                             |
| Other tools | : | lazydocker, htop, DuckDB |

## Luồng dữ liệu di chuyển (data lineage flow)

### giai đoạn 0: thu thập dữ liệu (data ingestion)
Dữ liệu di chuyển từ nguồn (API, file csv, file json...) -> lưu vào ./Data/raw

### giai đoạn 1: khai phá dữ liệu (data mining)

Dữ liệu sẽ được đọc từ nguồn (Data Source), được khai phá bằng `jupyter-all-spark`, viết thử light / heavy transform, refactor code rồi đóng gói lại thành các hàm để sử dụng trong file .py

### giai đoạn 2: xử lý dữ liệu (data processing)

Vẫn giống như trên, tuy nhiên viết chỉnh chu hơn, đóng gói thành các hàm, viết docstring, chia thành các section. dùng `spark-submit` và đánh giá performance, hiệu năng xử lý 36M dòng => 1.2M dòng => bắn vào database (Mysql) thông qua JDBC.

### giai đoạn 3: 

Kiểm tra dữ liệu trên database bằng datagrips (Done) \
nghịch thử DuckDB (Done) \


## Overview

## Architecture
(diagram)

## Technologies Used

## Data Flow

## ETL Logic

## Schema

## Run Instructions

## Folder Structure

## Author / Version