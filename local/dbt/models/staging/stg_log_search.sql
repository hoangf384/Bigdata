{{ config(materialized='view') }}

SELECT *
FROM read_parquet('~/Bigdata/local/data/raw/log_search/**/*.parquet')
