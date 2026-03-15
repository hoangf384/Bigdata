# GCP Transition Blueprint: Medallion Architecture (Bronze -> Silver -> Gold)

This document serves as the foundational architectural mandate for the transition from local DuckDB to a GCP-native Big Data stack (BigQuery + GCS + Spark + dbt).

## 1. Project Structure (Blueprint)

```text
Bigdata/
├── cloud/                      # Production logic for GCP
│   ├── ingestion/              # [BRONZE LAYER] - PySpark Ingestion Logic
│   │   ├── jobs/               # Spark jobs per source (logs, users, months)
│   │   ├── schemas/            # Spark StructType definitions for schema enforcement
│   │   └── utils/              # GCP helpers & common Spark functions
│   │
│   ├── dbt/                    # [SILVER & GOLD LAYER] - Transformation logic
│   │   ├── models/
│   │   │   ├── staging/        # [SILVER - Base] External Tables (GCS Parquet)
│   │   │   ├── intermediate/   # [SILVER - Clean] Joins & deduplication logic
│   │   │   └── marts/          # [GOLD - Business] Final Tables for BI
│   │   │       └── core/       # Customer 360, Fact/Dim tables
│   │   ├── profiles.yml        # BigQuery OAuth/Service Account config
│   │   └── dbt_project.yml     # Materialization: Marts must be 'table'
│   │
│   ├── terraform/              # Infrastructure as Code (GCS, BQ, Dataproc)
│   └── notebooks/              # Cloud-based EDA (Vertex AI)
│
├── local/                      # Legacy / Prototyping Environment
│   ├── data/                   # Local raw files (CSV, JSON, Parquet)
│   └── notebooks/              # Local prototype logic
│
├── scripts/                    # DevOps & Environment setup scripts
├── requirements.txt            # Python dependencies (pyspark, dbt-bigquery, etc.)
└── GEMINI.md                   # This architectural guide
```

## 2. Data Flow Mandates

1.  **Bronze (Ingestion):** PySpark MUST perform schema enforcement and convert raw formats (CSV/JSON) into **Parquet** on GCS. This layer handles the "heavy lifting" of file processing.
2.  **Silver (Staging/Inter):** dbt Staging models MUST point to BigQuery **External Tables** (reading GCS Parquet). Intermediate models perform deduplication and cross-source joins.
3.  **Gold (Marts):** Marts MUST be materialized as **Tables** in BigQuery to optimize performance for Looker/BI tools.
4.  **Security:** No secrets or JSON keys in the repository. Use GCP OAuth or IAM-based authentication.

## 3. Operational Rules

-   Always validate schemas in the Spark layer before uploading to GCS.
-   Marts should be documented with YAML descriptions for downstream BI clarity.
-   Use `dbt test` at the Silver layer to ensure data integrity before calculating Gold metrics.
