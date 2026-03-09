SELECT
    CAST(user_id AS VARCHAR) AS user_id,
    contentName AS content_name,
    screenName AS screen_name,
    CAST(TotalView AS BIGINT) AS total_view,
FROM {{ source('raw', 'thang6') }}
