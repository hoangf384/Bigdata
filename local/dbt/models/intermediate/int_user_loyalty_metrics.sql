WITH activity_dates AS (
    -- Ngày từ search
    SELECT user_id, CAST(search_datetime AS DATE) as activity_date FROM {{ ref('fct_search_logs') }}
    UNION
    -- Ngày từ content (tháng 4)
    SELECT u.user_id, c.log_date as activity_date 
    FROM {{ ref('fct_content_logs') }} c
    JOIN {{ ref('int_contracts') }} u ON c.contract = u.contract
    UNION
    -- Ngày từ views (lấy ngày đầu tháng)
    SELECT user_id, strptime(report_month || '-01', '%m-%d')::DATE as activity_date 
    FROM {{ ref('fct_user_views') }}
),

user_timeline AS (
    SELECT 
        user_id,
        MIN(activity_date) as first_active_date,
        MAX(activity_date) as last_active_date,
        COUNT(DISTINCT activity_date) as total_active_days
    FROM activity_dates
    GROUP BY 1
)

SELECT 
    t.*,
    u.created_date as registration_date,
    -- Giả định ngày báo cáo là 2022-08-01 (sau tháng 7)
    DATEDIFF('day', t.last_active_date, '2022-08-01'::DATE) as days_since_last_activity,
    DATEDIFF('day', u.created_date, t.last_active_date) as tenure_days
FROM user_timeline t
JOIN {{ ref('int_users') }} u ON t.user_id = u.user_id