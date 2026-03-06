{{ config(materialized='table') }}

SELECT
    contract,
    Mac,
    user_id
FROM read_parquet('~/Bigdata/local/data/raw/contracts/dim_contract_mock.parquet')