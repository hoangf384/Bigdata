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

## Luồng dữ liệu di chuyển (data lineage flow)

### giai đoạn 0: thu thập dữ liệu (data ingestion)
> dữ liệu di chuyển từ nguồn (API, file csv, file json...) -> lưu vào ./Data/raw

### giai đoạn 1: kiểm thử dữ liệu (data testing)
> dữ liệu di chuyển từ ./Data/raw -> đọc bằng jupyter-notebook -> xử lý dữ liệu và chạy thử kết quả thành công như mong đợi thì refactor lại, đóng gói thành file .py

### giai đoạn 2: xử lý dữ liệu (data processing)
> dữ liệu di chuyển từ ./Data/raw -> xử lý bằng spark (sử dụng spark-submit) -> lưu kết quả vào ./Data/destination \
> hầu như đến bước này mình đã kết thúc luồng, vì dữ liệu chỉ là file log, k có nguồn đầu vào đầu ra rõ ràng, và hiện tại mình chỉ mới học đến đây. Mình sẽ cập nhật sau




### giai đoạn 3: 

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