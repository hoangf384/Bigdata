WITH base AS (
    SELECT
        _id AS log_id,
        strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::DATE AS log_date,
        NULLIF(NULLIF(_source->>'$.Contract', 'NULL'), '0') AS contract,
        NULLIF(_source->>'$.Mac', 'NULL') AS mac_address,
        _source->>'$.AppName' AS app_name,
        CAST(_source->>'$.TotalDuration' AS BIGINT) AS total_duration,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY log_date DESC) as row_num
    FROM
        {{ source('raw', 'log_content') }}
    WHERE _source IS NOT NULL
)

SELECT
    * EXCLUDE(row_num)
FROM base
WHERE row_num = 1
  AND contract IS NOT NULL