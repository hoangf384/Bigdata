# How to Run Spark Applications

Tài liệu hướng dẫn cách khởi động cụm Spark và chạy các Spark apps trong môi trường Docker.

---

## 1. Khởi động cụm Spark

```bash
docker compose -f ~/Bigdata/infra/spark/docker-compose.yaml up -d
```

> Lệnh này khởi động: `spark-master`, `spark-worker-1`, `spark-worker-2`

Monitoring UI: [http://localhost:18080](http://localhost:18080)

---

## 2. Chạy Pipeline 1 – log_content ETL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars_external/mysql-connector-j-8.4.0.jar \
  /code/pipelines/log_content/etl_30_days.py
```

---

## 3. Chạy Pipeline 2a – log_search ETL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars_external/mysql-connector-j-8.4.0.jar \
  /code/pipelines/log_search/etl_log_search.py
```

---

## 4. Chạy LLM Enrichment (local Python)

```bash
cd ~/Bigdata
source .venv/bin/activate
python pipelines/log_search/enrich_v1.py
```

---

## 5. Chạy Spark category mapping

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /code/pipelines/log_search/mapping.py
```

---

## 6. Chạy post-enrich → MySQL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars_external/mysql-connector-j-8.4.0.jar \
  /code/pipelines/log_search/post_enrich.py
```

---

## 7. DuckDB UI

```bash
cd ~/Bigdata/Data && duckdb ~/Bigdata/DuckDB/db/mydatabase.duckdb -ui
```

---

## Lưu ý về JDBC JARs

Volume mount trong `docker-compose.yaml`:
```yaml
- ~/Bigdata/infra/spark/jars:/opt/spark/jars_external
```
Cần pass `--jars /opt/spark/jars_external/mysql-connector-j-8.4.0.jar` khi submit job có kết nối MySQL.
