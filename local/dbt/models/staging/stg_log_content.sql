{{ config(materialized='view') }}

SELECT 
    _id AS log_id,
    -- Dùng hàm filename() của DuckDB thay vì tham số của read_json
    strptime(regexp_extract(filename(), '(\d{8})', 1), '%Y%m%d')::DATE AS log_date,
    _source->>'$.Contract' AS contract,
    _source->>'$.Mac' AS mac_address,
    _source->>'$.AppName' AS app_name,
    CAST(_source->>'$.TotalDuration' AS BIGINT) AS total_duration
FROM
    {{ source('raw', 'log_content') }}
WHERE _source IS NOT NULL
