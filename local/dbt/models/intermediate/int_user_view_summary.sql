WITH base AS (
    SELECT 
        user_id,
        total_view,
        report_month
    FROM {{ ref('fct_user_views') }}
)

SELECT
    user_id,
    SUM(total_view) AS total_views_all_time,
    COUNT(DISTINCT report_month) AS active_months_count,
    MAX(report_month) AS latest_view_month
FROM base
GROUP BY 1