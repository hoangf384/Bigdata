{{ config(materialized='view') }}

SELECT *
FROM read_csv("~/Bigdata/local/data/raw/users/big_data_userid.csv", ignore_errors=true)
