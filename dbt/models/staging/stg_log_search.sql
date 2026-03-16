WITH base AS (
    SELECT
        eventID as event_id,
        SAFE_CAST(LEFT(datetime, 23) AS TIMESTAMP) as datetime,
        NULLIF(TRIM(CAST(user_id AS STRING)), 'NULL') as user_id,
        keyword,
        category,
        proxy_isp,
        platform,
        networkType as network_type,
        `action` as action,
        userPlansMap as user_plans_map,
        ROW_NUMBER() OVER (PARTITION BY eventID ORDER BY datetime DESC) as row_num
    FROM {{ source('raw', 'log_search') }}
)

SELECT
    * EXCEPT(row_num)
FROM base
WHERE
datetime IS NOT NULL
AND
user_id IS NOT NULL
AND
row_num = 1
