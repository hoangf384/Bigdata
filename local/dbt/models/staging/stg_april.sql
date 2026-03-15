SELECT
    NULLIF(TRIM(CAST(user_id AS VARCHAR)), 'NULL') AS user_id,
    contentName AS content_name,
    screenName AS screen_name,
    CAST(TotalView AS BIGINT) AS total_view
FROM {{ source('raw', 'thang4') }}
WHERE user_id IS NOT NULL