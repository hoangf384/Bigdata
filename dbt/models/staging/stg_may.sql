SELECT
    NULLIF(TRIM(CAST(user_id AS STRING)), 'NULL') AS user_id,
    contentName AS content_name,
    screenName AS screen_name,
    CAST(TotalView AS INT64) AS total_view
FROM {{ source('raw', 'thang5') }}
WHERE user_id IS NOT NULL