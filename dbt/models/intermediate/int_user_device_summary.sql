WITH search_activity AS (
    SELECT 
        user_id,
        MAX(search_datetime) as last_active
    FROM {{ ref('fct_search_logs') }}
    GROUP BY 1
),

contract_mac AS (
    SELECT 
        user_id,
        mac AS device_id,
        'Contract Hardware' AS device_type
    FROM {{ ref('int_contracts') }}
    WHERE mac IS NOT NULL
)

SELECT
    u.user_id,
    -- Lấy Mac address từ hợp đồng làm thiết bị chính
    MAX(c.device_id) AS primary_mac_address,
    MAX(s.last_active) AS last_search_at
FROM {{ ref('int_users') }} u
LEFT JOIN search_activity s ON u.user_id = s.user_id
LEFT JOIN contract_mac c ON u.user_id = c.user_id
GROUP BY 1
