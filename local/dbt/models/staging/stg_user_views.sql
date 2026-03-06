{{ config(materialized='view') }}

WITH april AS (
    SELECT 
        CAST(user_id AS VARCHAR) AS user_id,
        contentName AS content_name,
        screenName AS screen_name,
        CAST(TotalView AS BIGINT) AS total_view,
        4 AS report_month
    FROM {{ ref('stg_april') }}
),
may AS (
    SELECT 
        CAST(user_id AS VARCHAR) AS user_id,
        contentName AS content_name,
        screenName AS screen_name,
        CAST(TotalView AS BIGINT) AS total_view,
        5 AS report_month
    FROM {{ ref('stg_may') }}
),
june AS (
    SELECT 
        CAST(user_id AS VARCHAR) AS user_id,
        contentName AS content_name,
        screenName AS screen_name,
        CAST(TotalView AS BIGINT) AS total_view,
        6 AS report_month
    FROM {{ ref('stg_june') }}
)

SELECT * FROM april
UNION ALL
SELECT * FROM may
UNION ALL
SELECT * FROM june
