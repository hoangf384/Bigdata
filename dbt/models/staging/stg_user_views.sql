WITH april AS (
    SELECT
        *,
        4 AS report_month
    FROM {{ ref('stg_april') }}
),
may AS (
    SELECT
        *,
        5 AS report_month
    FROM {{ ref('stg_may') }}
),
june AS (
    SELECT
        *,
        6 AS report_month
    FROM {{ ref('stg_june') }}
)

SELECT * FROM april
UNION ALL
SELECT * FROM may
UNION ALL
SELECT * FROM june
