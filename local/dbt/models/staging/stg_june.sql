{{ config(materialized='view') }}

SELECT *
FROM read_csv('~/Bigdata/local/data/raw/months/thang6.csv',
              ignore_errors=true)
