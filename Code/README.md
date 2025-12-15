
# How to Run Spark Applications
Tài liệu này hướng dẫn cách khởi động cụm Spark và chạy Spark apps trong môi trường Docker.

# Mục lục
1. [Khởi động cụm Spark](#1-khởi-động-cụm-spark)  
2. [Chạy Spark App](#2-chạy-spark-app)

## 1. Khởi động cụm Spark
Trước khi chạy Spark applications, bạn cần khởi động cụm Spark. Có 2 cách:


### Cách 1 — Khởi động bằng Terminal

Mở terminal và chạy:

```bash
docker compose -f ~/Bigdata/Spark/docker-compose.yaml up -d
```

> Lệnh này sẽ khởi động toàn bộ service: `spark-master`, `spark-worker`
---

### Cách 2 — Khởi động bằng Visual Studio Code

1. Mở Visual Studio Code
2. Cài extension Docker
3. Mở tab Containers
4. Tìm container tên spark-master
5. Chuột phải, Start Container
> Cách này phù hợp nếu bạn thao tác chủ yếu trong VS Code.

---

## 2. Chạy Spark App

Thông thường chạy bằng terminal sẽ tiện hơn.

---

### Bước 1: Truy cập vào bash spark-master

```bash
docker exec -it spark-master /bin/bash
```


### Bước 2: Chạy Spark app bằng spark-submit

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /code/spark-apps/ETL_30_days_v2.py
```


### Bước 3: Monitoring
Mở trình duyệt và truy cập vào địa chỉ: [localhost:8080](http://localhost:18080) để theo dõi trạng thái của cụm Spark và các ứng dụng đang chạy. Mình đổi port từ 8080 > 18080 để tránh xung đột với jupyter-notebook của mình, bạn tự đổi lại nha.

### mount thêm jars
trước tiên thì phải thêm jars từ bên ngoài đã, trong file compose của mình có mục `- /home/hp/Bigdata/Spark/jars:/opt/spark/jars_external` đây là mục mình mount thêm thư viện jdbc cho spark kết nối, mình thử nhiều cách rồi nhưng mà k có cách nào tốt hết, chỉ có cách là thêm đường dẫn để bash tự đọc được, sau đây là cách mình mount nhé:
```bash

``` 

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars_external/mysql-connector-j-8.4.0.jar \
  /code/spark-apps/ETL_30_days_v3.py
```

