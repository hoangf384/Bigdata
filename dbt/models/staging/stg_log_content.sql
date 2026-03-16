WITH base AS (
    SELECT
        _id                AS log_id,
        CAST(log_date AS DATE) AS log_date,
        NULLIF(NULLIF(TRIM(contract), 'NULL'), '0') AS contract,
        NULLIF(TRIM(mac_address), 'NULL')           AS mac_address,
        app_name,
        CAST(total_duration AS INT64)               AS total_duration,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY log_date DESC) AS row_num
    FROM {{ source('raw', 'log_content') }}
    WHERE _id IS NOT NULL
)

SELECT
    log_id,
    log_date,
    contract,
    mac_address,
    app_name,
    total_duration
FROM base
WHERE row_num = 1
  AND contract IS NOT NULL
