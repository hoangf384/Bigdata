{{ config(materialized='view') }}

SELECT 
    _id AS log_id,
    strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::DATE AS log_date,
    _source->>'$.Contract' AS contract,
    _source->>'$.Mac' AS mac_address,
    _source->>'$.AppName' AS app_name,
    CAST(_source->>'$.TotalDuration' AS BIGINT) AS total_duration
FROM
    read_json('~/Bigdata/local/data/raw/log_content/*.json',
              filename = true,
              ignore_errors = true)
WHERE _source IS NOT NULL
