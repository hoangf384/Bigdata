WITH all_identities AS (
    -- Nguồn 1: Từ bảng đăng ký
    SELECT user_id FROM {{ ref('stg_user_registrations') }}
    
    UNION
    
    -- Nguồn 2: Từ bảng hợp đồng
    SELECT user_id FROM {{ ref('stg_contracts') }}
    
    UNION
    
    -- Nguồn 3: Từ bảng search
    SELECT user_id FROM {{ ref('stg_log_search') }}

    UNION

    -- Nguồn 4: Từ bảng views (Quan trọng nhất để fix 42M lỗi)
    SELECT user_id FROM {{ ref('stg_user_views') }}
),

registration_info AS (
    SELECT 
        user_id,
        MIN(created_date) as created_date
    FROM {{ ref('stg_user_registrations') }}
    GROUP BY 1
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['a.user_id']) }} as id,
    a.user_id,
    r.created_date
FROM all_identities a
LEFT JOIN registration_info r ON a.user_id = r.user_id