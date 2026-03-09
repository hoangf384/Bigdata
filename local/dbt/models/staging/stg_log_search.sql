WITH base AS (
    SELECT
        eventID as event_id,
        TRY_CAST(LEFT(datetime, 23) AS TIMESTAMP) as datetime,
        NULLIF(TRIM(CAST(user_id AS VARCHAR)), 'NULL') as user_id,
        keyword,
        category,
        proxy_isp,
        platform,
        networkType as network_type,
        "action" as action,
        userPlansMap as user_plans_map,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY datetime DESC) as row_num
    FROM {{ source('raw', 'log_search') }}
)

SELECT
    * EXCLUDE(row_num)
FROM base
WHERE
datetime IS NOT NULL
AND
user_id IS NOT NULL
AND
row_num = 1
