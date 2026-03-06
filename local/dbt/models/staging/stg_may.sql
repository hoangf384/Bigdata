{{ config(materialized='view') }}

SELECT *
FROM read_csv('~/Bigdata/local/data/raw/months/thang5.csv',
              ignore_errors=true)
