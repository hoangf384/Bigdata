WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY search_datetime DESC) as row_num
    FROM {{ source('raw', 'log_search') }}
)

SELECT
    * EXCLUDE(row_num)
FROM base
WHERE row_num = 1
