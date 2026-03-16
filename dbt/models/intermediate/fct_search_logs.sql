WITH cleaned AS (
    SELECT
        event_id,
        user_id,
        datetime AS search_datetime,
        keyword,
        category,
        platform,
        network_type
    FROM {{ ref('stg_log_search') }}
)

SELECT *
FROM cleaned
WHERE user_id IS NOT NULL
  AND search_datetime IS NOT NULL
