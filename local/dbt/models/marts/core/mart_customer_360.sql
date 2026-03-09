WITH user_spine AS (
    SELECT * FROM {{ ref('int_users') }}
),

contract_info AS (
    SELECT * FROM {{ ref('int_contracts') }}
),

content_stats AS (
    SELECT * FROM {{ ref('fct_customer_content_stats') }}
),

search_taste AS (
    SELECT * FROM {{ ref('fct_customer_search_taste') }}
),

view_summary AS (
    SELECT * FROM {{ ref('int_user_view_summary') }}
),

device_summary AS (
    SELECT * FROM {{ ref('int_user_device_summary') }}
),

loyalty_metrics AS (
    SELECT * FROM {{ ref('int_user_loyalty_metrics') }}
)

SELECT
    u.user_id,
    u.created_date AS registration_date,
    c.contract,
    d.primary_mac_address AS mac_address,
    d.search_platforms,
    
    -- Trạng thái thuê bao
    CASE WHEN c.contract IS NOT NULL THEN TRUE ELSE FALSE END AS is_subscriber,
    
    -- Hành vi xem chi tiết (Paid - Tháng 4)
    COALESCE(cs.active_days, 0) AS content_active_days_apr,
    cs.most_watch AS most_watched_category_apr,
    
    -- Hành vi xem tổng quát (Free+Paid)
    COALESCE(vs.total_views_all_time, 0) AS total_views_all_time,
    
    -- Hành vi tìm kiếm (Tháng 6,7)
    st.most_search_m7 AS latest_search_keyword,
    st.category_m7 AS latest_search_category,
    
    -- Chỉ số trung thành & hoạt động
    l.first_active_date,
    l.last_active_date,
    COALESCE(l.days_since_last_activity, 999) AS days_since_last_activity,
    COALESCE(l.total_active_days, 0) AS active_days_count,
    
    -- Phân lớp khách hàng thông minh
    CASE 
        WHEN c.contract IS NOT NULL AND l.days_since_last_activity <= 7 THEN 'VVIP - Active'
        WHEN c.contract IS NOT NULL AND l.days_since_last_activity > 30 THEN 'Subscriber - At Risk'
        WHEN c.contract IS NOT NULL THEN 'Subscriber - Healthy'
        WHEN l.total_active_days >= 10 THEN 'Loyal Guest'
        WHEN l.days_since_last_activity <= 7 THEN 'Active Guest'
        ELSE 'Casual Guest'
    END AS customer_lifecycle_segment

FROM user_spine u
LEFT JOIN contract_info c ON u.user_id = c.user_id
LEFT JOIN device_summary d ON u.user_id = d.user_id
LEFT JOIN content_stats cs ON c.contract = cs.contract
LEFT JOIN search_taste st ON u.user_id = st.user_id
LEFT JOIN view_summary vs ON u.user_id = vs.user_id
LEFT JOIN loyalty_metrics l ON u.user_id = l.user_id