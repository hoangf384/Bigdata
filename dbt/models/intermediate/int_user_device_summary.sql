WITH search_platforms AS (
    SELECT 
        user_id,
        platform,
        MAX(search_datetime) as last_active
    FROM {{ ref('fct_search_logs') }}
    GROUP BY 1, 2
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
    -- Gom nhóm các platform search
    STRING_AGG(DISTINCT s.platform, ', ') AS search_platforms,
    -- Lấy Mac address từ hợp đồng
    MAX(c.device_id) AS primary_mac_address,
    COUNT(DISTINCT s.platform) AS unique_platforms_count
FROM {{ ref('int_users') }} u
LEFT JOIN search_platforms s ON u.user_id = s.user_id
LEFT JOIN contract_mac c ON u.user_id = c.user_id
GROUP BY 1